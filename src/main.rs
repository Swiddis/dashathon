use chrono::{DateTime, Utc};
use octocrab::{models::issues::Issue, params::issues::Sort, Octocrab, Page};
use ratelimit::Ratelimiter;
use std::{env, time::Duration};
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};

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
    _e_type: EntityType,
    _id: u64,
}

/// Types of entities we care about requesting from the GitHub API
#[derive(Debug)]
enum DataRequest {
    IssuePage(u32, oneshot::Sender<Page<Issue>>),
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
        }
        Err(err) => {
            log::warn!("unable to fetch rate limit information from GitHub: \"{err}\", using default options");
            // Guess the PAT default rate limit (5000/hr), but start with 0 tokens
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

async fn get_issue_page(client: &Octocrab, page: u32) -> Page<Issue> {
    let page = client
        .issues("opensearch-project", "opensearch-dashboards")
        .list()
        .sort(Sort::Created)
        .per_page(100)
        .page(page)
        .send()
        .await;
    page.unwrap() // TODO handle err
}

async fn handle_data_request(client: &Octocrab, req: DataRequest) {
    log::debug!("Received request: {req:?}");
    match req {
        DataRequest::IssuePage(page, sender) => {
            let result = get_issue_page(client, page).await;
            let _ = sender.send(result);
        }
    }
}

/// Spawn a task to handle all requests to the GitHub API, respecting rate limiting. We keep this as
/// a dedicated task to respect GitHub's published best practices to avoid parallel requests.
fn start_request_handling(
    client: Octocrab,
    mut requests: mpsc::Receiver<DataRequest>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let limiter = github_ratelimiter(&client).await;
        log::info!("starting main request loop");

        loop {
            if let Err(sleep) = limiter.try_wait() {
                tokio::time::sleep(sleep).await;
            }

            match requests.recv().await {
                Some(req) => handle_data_request(&client, req).await,
                None => break,
            }
        }
    })
}

/// Spawn a task to keep track of the newest events in GH, and update the corresponding database
/// records. This is the bulk of the long-term scraping and should typically never halt.
fn start_update_tracking(
    _requests: mpsc::Sender<DataRequest>,
    _updates: mpsc::Sender<EntityUpdate>,
    _track_start: DateTime<Utc>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        log::info!("starting main update loop");

        loop {
            // TODO use Events API to detect updates and put in SQLite store
            break;
        }
    })
}

/// Spawn a task to backfill any missing data from before the startup time. Terminates when there's
/// nothing left to fill.
fn start_backfill(
    requests: mpsc::Sender<DataRequest>,
    updates: mpsc::Sender<EntityUpdate>,
    ignore_updated_after: DateTime<Utc>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut curr_page = 1;
        let mut has_next = true;

        log::info!("starting main backfill loop");

        while has_next {
            let (send, rec) = oneshot::channel();
            // If the request handler stops running and we get errors while communicating with it,
            // just shutdown this task silently
            let Ok(_) = requests.send(DataRequest::IssuePage(curr_page, send)).await else {
                return;
            };
            let Ok(page) = rec.await else { return };

            has_next = page.next.is_some();
            for issue in page
                .into_iter()
                .filter(|i| i.updated_at < ignore_updated_after)
            {
                updates
                    .send(EntityUpdate {
                        _e_type: EntityType::Issue,
                        _id: *issue.id,
                    })
                    .await
                    .unwrap();
            }
            curr_page += 1;
        }
    })
}

/// Preprocess and upload records found in scrape tasks to OpenSearch.
fn start_uploading(mut receiver: mpsc::Receiver<EntityUpdate>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        log::info!("starting main upload loop");

        while let Some(update) = receiver.recv().await {
            log::trace!("processing entity update: {update:?}");
        }
    })
}

#[tokio::main]
async fn main() -> Result<(), MetricCollectionError> {
    env_logger::init();

    let client = octocrab_client()?;
    let (request_sender, request_receiver) = mpsc::channel::<DataRequest>(32);
    let (scrape_sender, scrape_receiver) = mpsc::channel::<EntityUpdate>(32);
    let now = Utc::now();

    log::info!("successfully set up GitHub client and channels, spawning main tasks");

    let result = tokio::try_join!(
        start_request_handling(client, request_receiver),
        start_update_tracking(request_sender.clone(), scrape_sender.clone(), now.clone()),
        start_backfill(request_sender, scrape_sender, now),
        start_uploading(scrape_receiver),
    );

    log::info!("main task batch has terminated, shutting down");

    // In normal operation, the above join never terminates. Handle any errors that came up.
    match result {
        Ok(_) => Ok(()),
        Err(err) => Err(MetricCollectionError::TaskRuntimeError {
            message: err.to_string(),
        }),
    }
}
