// Some error variants (in particular those from Octocrab) are large, which causes a performance hit
// while calling fallible methods. Since the application isn't performance-critical and the
// workaround is annoying, we silence the associated lint project-wide.
#![allow(clippy::result_large_err)]

use octocrab::Octocrab;
use std::env;
use thiserror::Error;

/// MetricCollectionError represents the top-level error type of the application. This holds the
/// full set of errors that the application may exit with.
#[derive(Error, Debug)]
pub enum MetricCollectionError {
    #[error("failed to interact with the GitHub API")]
    GithubError(#[from] octocrab::Error),
}

fn octocrab_client() -> Result<Octocrab, octocrab::Error> {
    let token = env::var("GH_TOKEN").unwrap_or("".into());
    let mut builder = octocrab::OctocrabBuilder::new();
    if !token.is_empty() {
        builder = builder.personal_token(token);
    };
    builder.build()
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), MetricCollectionError> {
    let octocrab = octocrab_client()?;
    let page = octocrab
        .issues("opensearch-project", "OpenSearch-Dashboards")
        .list()
        .per_page(50)
        .send()
        .await?;

    for issue in &page {
        println!("{}: {}", issue.number, issue.title);
    }

    Ok(())
}
