use anyhow::Result;
use idk::context::Context;
use idk::printdbg;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::io::BufReader;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

#[tokio::main(flavor = "multi_thread", worker_threads = 10)]
async fn main() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:8080").await?;
    let next_client_id = Arc::new(AtomicUsize::new(1));

    loop {
        let (socket, _) = listener.accept().await?;
        let client_id = next_client_id.fetch_add(1, Ordering::SeqCst);

        tokio::spawn(handle_client(socket, client_id));
    }
}

async fn handle_client(socket: TcpStream, client_id: usize) {
    let mut ctx = Context::default();
    let (reader, mut writer) = socket.into_split();
    let mut reader = BufReader::new(reader);
    let mut buffer = String::new();

    println!("Client {} connected!", client_id);
    let _ = writer
        .write_all(format!("Client {} connected!\n", client_id).as_bytes())
        .await;

    loop {
        printdbg!("Awaiting query...");
        let _ = writer.write_all("> ".as_bytes()).await;

        buffer.clear();
        let bytes_read = reader.read_line(&mut buffer).await.unwrap();

        if bytes_read == 0 {
            let _ = writer
                .write_all(format!("Client {} disconnected.\n", client_id).as_bytes())
                .await;
            break;
        }

        let query = buffer.trim();
        printdbg!("Query: {}", query);
        if query.eq_ignore_ascii_case("quit") {
            let _ = writer
                .write_all(format!("Goodbye, Client {}!\n", client_id).as_bytes())
                .await;
            break;
        }

        match ctx.execute_sql(query) {
            Ok(result) => {
                if result.is_empty() {
                    continue;
                }
                let _ = writer.write_all(result.print().as_bytes()).await;
            }
            Err(e) => {
                let _ = writer
                    .write_all(format!("Error executing query: {}\n", e).as_bytes())
                    .await;
            }
        }
    }
}
