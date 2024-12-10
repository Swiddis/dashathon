use thiserror::Error;

/// Metric collection error represents the top-level error type of this metric collector. This holds
/// the full set of errors that the application may exit with.
#[derive(Error, Debug)]
pub enum MetricCollectionError {
    #[error("failed to interact with the github API")]
    GithubError(#[from] octocrab::Error),
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), MetricCollectionError> {
    let octocrab = octocrab::instance();
    let page = octocrab.issues("opensearch-project", "OpenSearch-Dashboards")
        .list()
        .per_page(50)
        .send()
        .await?;

    for issue in &page {
        println!("{}: {}", issue.number, issue.title);
    }

    Ok(())
}
