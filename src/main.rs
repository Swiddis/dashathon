use chrono::{DateTime, Utc};
use octocrab::{
    models::issues::Issue,
    params::{issues::Sort, State},
    Octocrab, Page,
};
use opensearch::{http::StatusCode, BulkParts, OpenSearch};
use ratelimit::Ratelimiter;
use serde_json::json;
use std::{env, time::Duration};
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};

/// CollectionError represents the top-level error type of the application, with
/// all its failure modes
#[derive(Error, Debug)]
enum CollectionError {
    #[error("failed to configure {context} during startup: {message}")]
    ConfigError { context: String, message: String },
    #[error("preflight failed for {context}: {message}")]
    PreflightError { context: String, message: String },
    #[error("a task terminated unexpectedly: {message}")]
    TaskPanic { message: String },
}

/// Update messages for GitHub entities
#[derive(Debug, Clone)]
enum EntityUpdate {
    Issue(Box<Issue>),
}

impl EntityUpdate {
    fn bulk_entry(&self) -> Option<[String; 2]> {
        let serialized: Result<String, serde_json::Error>;
        let index_entry: String;

        match self {
            EntityUpdate::Issue(ref issue) => {
                serialized = serde_json::to_string(&issue);
                index_entry =
                    json!({ "index": { "_index": "github_issues", "_id": *issue.id } }).to_string();
            }
        };

        match serialized {
            Ok(s) => Some([index_entry, s]),
            Err(err) => {
                log::error!("unserializable update ({err}): {self:?}");
                None
            }
        }
    }
}

/// Types of entities we care about requesting from the GitHub API
#[derive(Debug)]
enum DataRequest {
    IssuePage(u32, oneshot::Sender<Page<Issue>>),
}

async fn github_ratelimiter(client: &Octocrab) -> Result<Ratelimiter, CollectionError> {
    let limits = client.ratelimit().get().await;
    match limits {
        Ok(limits) => {
            let refresh_ivl = Duration::from_secs(3600).div_f64(limits.rate.limit as f64);
            Ok(Ratelimiter::builder(1, refresh_ivl)
                .max_tokens(limits.rate.limit as u64)
                .initial_available(limits.rate.remaining as u64)
                .build()
                .unwrap_or_else(|_| {
                    panic!(
                        "GitHub returned an illegal rate limit state: {:?}",
                        limits.rate
                    )
                }))
        }
        Err(err) => Err(CollectionError::PreflightError {
            context: "GitHub".to_string(),
            message: err.to_string(),
        }),
    }
}

/// Since handling persistent errors after startup is annoying, we run a
/// preflight to check the config before really getting started.
async fn do_preflight(
    opensearch: OpenSearch,
    octocrab: Octocrab,
) -> Result<Ratelimiter, CollectionError> {
    log::info!("Running preflight");

    let (os_result, octo_result) = tokio::join!(
        tokio::spawn(async move { opensearch.info().send().await }),
        tokio::spawn(async move { github_ratelimiter(&octocrab).await }),
    );

    match os_result {
        Ok(Ok(_)) => {}
        Ok(Err(err)) => {
            return Err(CollectionError::PreflightError {
                context: "OpenSearch".to_string(),
                message: err.to_string(),
            })
        }
        Err(err) => {
            return Err(CollectionError::PreflightError {
                context: "Tokio runtime".to_string(),
                message: err.to_string(),
            })
        }
    };

    match octo_result {
        Ok(Ok(lim)) => Ok(lim),
        Ok(Err(err)) => Err(err),
        Err(err) => Err(CollectionError::PreflightError {
            context: "Tokio runtime".to_string(),
            message: err.to_string(),
        }),
    }
}

fn octocrab_client() -> Result<Octocrab, CollectionError> {
    // TODO probably should be config instead of env.
    let token = env::var("GH_TOKEN").unwrap_or("".to_string());
    let mut builder = octocrab::OctocrabBuilder::new();
    if !token.is_empty() {
        builder = builder.personal_token(token);
    };
    let result = builder.build();
    match result {
        Ok(client) => Ok(client),
        Err(err) => Err(CollectionError::ConfigError {
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
        .state(State::All)
        .per_page(100)
        .page(page)
        .send()
        .await;
    page.unwrap() // TODO handle err
}

async fn handle_data_request(client: &Octocrab, req: DataRequest) {
    // TODO ideally we should cache results in a local file to recover quickly from a cache, but not
    // P0 since otherwise we can just backfill everything again (mostly annoying since many comments
    // may bump the rate limit)
    log::debug!("received request: {req:?}");
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
    limiter: Ratelimiter,
    mut requests: mpsc::Receiver<DataRequest>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        log::debug!("starting main request loop");
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
        log::debug!("starting main update loop");
        // TODO use Events API to detect updates
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
        // TODO we currently only backfill the issues, also need to backfill PRs, comments, and
        // other info
        let mut curr_page = 1;
        let mut has_next = true;

        log::debug!("starting main backfill loop");

        while has_next {
            let (send, rec) = oneshot::channel();
            let Ok(_) = requests.send(DataRequest::IssuePage(curr_page, send)).await else {
                // If the request handler is no longer running, there's nothing left to do
                return;
            };
            let Ok(page) = rec.await else { return };

            has_next = page.next.is_some();
            for issue in page
                .into_iter()
                .filter(|i| i.updated_at < ignore_updated_after)
            {
                updates
                    .send(EntityUpdate::Issue(Box::new(issue)))
                    .await
                    .unwrap();
            }
            curr_page += 1;
        }

        log::info!("backfill complete!");
    })
}

async fn upload_entries(client: &OpenSearch, updates: &[EntityUpdate]) {
    log::debug!("uploading {} entity update(s)", updates.len());
    let updates = updates
        .iter()
        .filter_map(|upd| upd.bulk_entry())
        .flatten()
        .collect();

    // TODO handle error
    let response = client
        .bulk(BulkParts::None)
        .body(updates)
        .send()
        .await
        .unwrap();
    match response.status_code() {
        StatusCode::OK => {}
        code => {
            log::error!(
                "encountered error from OpenSearch ({code}): {}",
                response.text().await.unwrap()
            );
            todo!("handle error")
        }
    };
}

/// Preprocess and upload records found in scrape tasks to OpenSearch.
fn start_uploading(
    client: OpenSearch,
    mut receiver: mpsc::Receiver<EntityUpdate>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        log::debug!("starting main upload loop");
        let mut buffer = Vec::new();

        loop {
            let updates = receiver.recv_many(&mut buffer, 128).await;
            if updates == 0 {
                // No messages are left and the upstream channels have closed, shut down gracefully
                return;
            }
            upload_entries(&client, &buffer).await;
            buffer.clear();
        }
    })
}

async fn run() -> Result<(), CollectionError> {
    let octo_client = octocrab_client()?;
    let opensearch_client = OpenSearch::default(); // TODO config

    let limiter = do_preflight(opensearch_client.clone(), octo_client.clone()).await?;

    log::info!("successfully set up clients, spawning main tasks");

    let (request_sender, request_receiver) = mpsc::channel::<DataRequest>(16);
    let (scrape_sender, scrape_receiver) = mpsc::channel::<EntityUpdate>(256);
    // We use the current time as a pivot point to cleanly separate new events and backfilling.
    let now = Utc::now();

    let result = tokio::try_join!(
        start_request_handling(octo_client, limiter, request_receiver),
        start_update_tracking(request_sender.clone(), scrape_sender.clone(), now),
        start_backfill(request_sender, scrape_sender, now),
        start_uploading(opensearch_client, scrape_receiver),
    );

    log::info!("main task batch has terminated, shutting down");

    // In normal operation, the above join never terminates. Handle any errors that came up.
    match result {
        Ok(_) => Ok(()),
        Err(err) => Err(CollectionError::TaskPanic {
            message: err.to_string(),
        }),
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> std::process::ExitCode {
    env_logger::init();

    let result = run().await;

    match result {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(err) => {
            log::error!("{err}");
            std::process::ExitCode::FAILURE
        }
    }
}
