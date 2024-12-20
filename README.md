# Dashathon: a GitHub Metrics Collector

WIP -- This project is a small Rust-based application that collects metrics from GitHub APIs and
uploads the results to OpenSearch. In the future, this project will upload more sophisticated
derived data, as well as include some dashboards to use for insights on large repositories.

## Prerequisites

- Rust (latest stable version)
- An OpenSearch instance running locally
- GitHub Personal Access Token (PAT)

## Setup and Running

1. Clone the repository:
   ```
   git clone https://github.com/Swiddis/dashathon.git
   cd dashathon
   ```

2. Set up your GitHub Personal Access Token:
   ```
   export GH_TOKEN=your_github_personal_access_token
   ```

3. Ensure you have a local OpenSearch instance running without authentication.

4. Build and run the project:
   ```
   cargo build --release
   RUST_LOG=info ./target/release/dashathon-collector
   ```

   You can adjust the log level by changing `info` to `debug`, `warn`, or `error` as needed.

## Configuration

- `GH_TOKEN`: Set this environment variable to your GitHub Personal Access Token.
- `RUST_LOG`: Controls the logging level. Options are `error`, `warn`, `info`, `debug`, and `trace`.

## Notes

- The application currently uses a local OpenSearch instance without authentication. Future versions
  may include more robust authentication and configuration options.
- The repository to scrape from is hardcoded for now, too.
- The project is WIP, and will panic if you breathe on it the wrong way. (Still working on figuring
  out a robust error handling strategy with Tokio.)

## Contributing

This is part of a contest hackathon that runs until 30 December, 2024, so contributions won't be
accepted until after the deadline (except from team members). After that date, contributions are
welcome.

## License

This project is licensed under the [Apache v2.0 License](LICENSE.txt).
