use anyhow::Result;
use idk::local_server::entry;

// default binary is the local server, lambda binary is idk-lambda
#[tokio::main(flavor = "multi_thread", worker_threads = 10)]
async fn main() -> Result<()> {
    entry().await
}
