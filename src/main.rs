use octocrab::{models::issues::Issue, Octocrab, Page};
use ratelimit::Ratelimiter;
use std::{env, sync::Arc, time::{Duration, Instant}};
use thiserror::Error;
use tokio::{sync::{mpsc::{self, Receiver}, oneshot}, task::JoinError};

/// MetricCollectionError represents the top-level error type of the application, with all its
/// failure modes
#[derive(Error, Debug)]
enum MetricCollectionError {
    #[error("failed to configure {context} during startup: {message}")]
    ConfigError { context: String, message: String },
    #[error("a task terminated unexpectedly: {message}")]
    TaskRuntimeError { message: String },
}

#[derive(Debug, Clone, Copy)]
enum EntityType {
    Issue,
}

/// Update messages for GitHub entities
#[derive(Debug, Clone)]
struct EntityUpdate {
    e_type: EntityType,
    id: u64,
}

/// Types of entities we care about requesting from the GitHub API
#[derive(Debug)]
enum DataRequest {
    IssuePage(usize, oneshot::Sender<Page<Issue>>),
    Issue(usize, oneshot::Sender<Issue>),
}

async fn github_ratelimiter(client: &Octocrab) -> Ratelimiter {
    let limits = client.ratelimit().get().await;
    match limits {
        Ok(limits) => {
            let refresh_ivl = Duration::from_secs(3600).div_f64(limits.rate.limit as f64);
            Ratelimiter::builder(1, refresh_ivl)
                .max_tokens(limits.rate.limit as u64)
                .initial_available(limits.rate.remaining as u64)
                .build()
                .expect("GitHub returned an invalid rate limit state: {limits:?}")
        },
        Err(err) => {
            log::warn!("unable to fetch rate limit information from GitHub: {err}");
            // If we aren't able to fetch the rate limit, just guess based on the PAT rate limit of
            // 5000 requests/hr, but start with 0 tokens.
            Ratelimiter::builder(1, Duration::from_millis(720))
                .max_tokens(5000)
                .build()
                .expect("unreachable: hardcoded valid rate limit parameters")
        }
    }
}

fn octocrab_client() -> Result<Octocrab, MetricCollectionError> {
    // TODO probably should be config instead of env. We require a PAT to work with the rate limiter.
    let token = env::var("GH_TOKEN")
        .expect("a personal access token must be provided via the GH_TOKEN environment variable");
    let mut builder = octocrab::OctocrabBuilder::new();
    if !token.is_empty() {
        builder = builder.personal_token(token);
    };
    let result = builder.build();
    match result {
        Ok(client) => Ok(client),
        Err(err) => Err(MetricCollectionError::ConfigError {
            context: "Octocrab client".to_string(),
            message: err.to_string(),
        }),
    }
}

fn handle_data_request(req: DataRequest) {
    log::debug!("Received request: {req:?}")
}

/// Spawn a task to handle all requests to the GitHub API, respecting rate limiting. We keep this as
/// a dedicated task to respect GitHub's published best practices to avoid parallel requests.
fn start_request_handling(client: Octocrab, mut requests: Receiver<DataRequest>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let limiter = github_ratelimiter(&client).await;

        loop {
            if let Err(sleep) = limiter.try_wait() {
                tokio::time::sleep(sleep).await;
            }

            match requests.recv().await {
                Some(req) => handle_data_request(req),
                None => break,
            }
        }
    })
}

/// Spawn a task to keep track of the newest events in GH, and update the corresponding database
/// records. This is the bulk of the long-term scraping and should typically never halt.
fn start_update_tracking(requests: mpsc::Sender<DataRequest>, updates: mpsc::Sender<EntityUpdate>, track_start: &Instant) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            // TODO use Events API to detect updates and put in SQLite store
            updates.send(EntityUpdate { e_type: EntityType::Issue, id: 0 }).await.unwrap();
        }
    })
}

/// Spawn a task to backfill any missing data from before the startup time. Terminates when there's
/// nothing left to fill.
fn start_backfill(requests: mpsc::Sender<DataRequest>, updates: mpsc::Sender<EntityUpdate>, ignore_updated_after: &Instant)  -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            // TODO backfill all the GH data we care about (issues/comments/pulls)
            updates.send(EntityUpdate { e_type: EntityType::Issue, id: 1 }).await.unwrap();
        }
    })
}

/// Preprocess and upload records found in scrape tasks to OpenSearch.
fn start_uploading(mut receiver: mpsc::Receiver<EntityUpdate>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(message) = receiver.recv().await {
            log::debug!("{message:?}");
        }
    })
}

#[tokio::main(flavor="current_thread")]
async fn main() -> Result<(), MetricCollectionError> {
    let client = octocrab_client()?;
    let (request_sender, request_receiver) = mpsc::channel::<DataRequest>(32);
    let (scrape_sender, scrape_receiver) = mpsc::channel::<EntityUpdate>(32);
    let now = std::time::Instant::now();
    
    let result = tokio::try_join!(
        start_request_handling(client, request_receiver),
        start_update_tracking(request_sender.clone(), scrape_sender.clone(), &now),
        start_backfill(request_sender, scrape_sender, &now),
        start_uploading(scrape_receiver),
    );

    // In normal operation, the above join never terminates. Handle any errors that came up.
    match result {
        Ok(_) => Ok(()),
        Err(err) => Err(MetricCollectionError::TaskRuntimeError { message: err.to_string() }),
    }
}
