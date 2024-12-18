use octocrab::Octocrab;
use ratelimit::Ratelimiter;
use std::{env, sync::Arc, time::{Duration, Instant}};
use thiserror::Error;
use tokio::{sync::mpsc, task::JoinError};

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
    Comment,
    PullRequest,
}

/// Update messages for GitHub entities
#[derive(Debug, Clone)]
struct EntityUpdate {
    e_type: EntityType,
    id: u64,
}

fn github_ratelimiter() -> Ratelimiter {
    // We hardcode the GH PAT rate limit of 5000 requests/hr (no bursts). A more robust
    // solution would involve using the actual headers, Octocrab doesn't expose that
    // information so we work around it for now. We also start with 0 tokens to keep the
    // application well-behaved after a restart, this mostly matters during a cold start
    // when there's a lot to backfill.
    Ratelimiter::builder(1, Duration::from_millis(720))
        .max_tokens(5000)
        .build()
        .expect("unreachable: rate limit config is hardcoded as valid")
}

fn octocrab_client() -> Result<Octocrab, MetricCollectionError> {
    // TODO probably should be config instead of env. We require a PAT to work with the rate limiter.
    let token = env::var("GH_TOKEN")
        .expect("A personal access token must be provided via the GH_TOKEN environment variable");
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

fn start_update_tracking(client: Arc<Octocrab>, limiter: Arc<Ratelimiter>, sender: mpsc::Sender<EntityUpdate>, track_start: &Instant) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            // TODO use Events API to detect updates and put in SQLite store
            sender.send(EntityUpdate { e_type: EntityType::Issue, id: 0 }).await.unwrap();
        }
    })
}

fn start_backfill(client: Arc<Octocrab>, limiter: Arc<Ratelimiter>, sender: mpsc::Sender<EntityUpdate>, ignore_updated_after: &Instant)  -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            // TODO backfill all the GH data we care about (issues/comments/pulls)
            sender.send(EntityUpdate { e_type: EntityType::Issue, id: 1 }).await.unwrap();
        }
    })
}

fn start_uploading(mut receiver: mpsc::Receiver<EntityUpdate>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(message) = receiver.recv().await {
            println!("{message:?}");
        }
    })
}

#[tokio::main]
async fn main() -> Result<(), MetricCollectionError> {
    let client = Arc::new(octocrab_client()?);
    let limiter = Arc::new(github_ratelimiter());
    let (scrape_sender, mut scrape_receiver) = mpsc::channel::<EntityUpdate>(128);
    let now = std::time::Instant::now();
    
    let result = tokio::try_join!(
        start_update_tracking(client.clone(), limiter.clone(), scrape_sender.clone(), &now),
        start_backfill(client, limiter, scrape_sender, &now),
        start_uploading(scrape_receiver),
    );

    // In normal operation, the above join never terminates. Handle any errors that came up.
    match result {
        Ok(_) => Ok(()),
        Err(err) => Err(MetricCollectionError::TaskRuntimeError { message: err.to_string() }),
    }
}
