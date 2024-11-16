use anyhow::Result;
use idk::context::Context;
use idk::printdbg;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::io::BufReader;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
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

// TODO: Handle comment strings
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

        buffer = buffer.trim().to_string();

        if bytes_read == 0 {
            let _ = writer
                .write_all(format!("Client {} disconnected.\n", client_id).as_bytes())
                .await;
            break;
        }

        if buffer.eq_ignore_ascii_case("quit") {
            let _ = writer
                .write_all(format!("Goodbye, Client {}!\n", client_id).as_bytes())
                .await;
            break;
        }

        if buffer.is_empty() {
            continue;
        } else if buffer.starts_with("--") || buffer.starts_with("/*") {
            skip_comments(&mut buffer, &mut reader).await;
        } else {
            execute_query(&mut buffer, &mut ctx, &mut reader, &mut writer).await;
        }
    }
}

async fn skip_comments(buffer: &mut String, reader: &mut BufReader<OwnedReadHalf>) {
    if buffer.trim_start().starts_with("--") {
        buffer.clear();
    }
    if buffer.contains("*/") {
        buffer.clear();
    } else {
        while !buffer.contains("*/") {
            buffer.clear();
            if reader.read_line(buffer).await.unwrap_or(0) == 0 {
                break;
            }
        }
        buffer.clear();
    }
}

async fn execute_query(
    buffer: &mut String,
    ctx: &mut Context,
    reader: &mut BufReader<OwnedReadHalf>,
    writer: &mut OwnedWriteHalf,
) {
    let mut query = String::new();

    loop {
        if buffer.trim().ends_with(";") {
            query.push_str(&buffer[..buffer.len() - 1]);
            printdbg!("Query: {}", query);
            return match ctx.execute_sql(query.clone()) {
                Ok(result) => {
                    if !result.is_empty() || !result.get_info().is_empty() {
                        let _ = writer.write_all(result.print().as_bytes()).await;
                    }
                }
                Err(e) => {
                    let _ = writer.write_all(format!("Error: {}\n", e).as_bytes()).await;
                    panic!("{:?}", e);
                }
            };
        } else {
            query.push_str(buffer);
            buffer.clear();
            buffer.push(' '); // avoid concating words in different lines
            let _ = writer.write_all("... ".as_bytes()).await;
            let _ = reader.read_line(buffer).await.unwrap();
        }
    }
}
