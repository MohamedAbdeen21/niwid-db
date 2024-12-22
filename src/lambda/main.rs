use anyhow::Result;
use idk::context::Context;
use lambda_http::{run, service_fn, Body, Error, Request, RequestPayloadExt, Response};
use serde::{Deserialize, Serialize};

use lambda_runtime::diagnostic::Diagnostic;

#[tokio::main]
async fn main() -> Result<(), Diagnostic> {
    Ok(entry().await?)
}

pub async fn entry() -> Result<(), Error> {
    let func = service_fn(handle_client);
    run(func).await
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Item {
    pub query: String,
}

async fn handle_client(event: Request) -> Result<Response<Body>, Error> {
    println!("Received event: {:?}", event);

    let path = event.uri().path();

    println!("Path: {}", path);

    match path {
        "/" => serve_frontend().await,
        "/query" => execute_query(&event.json::<Item>().unwrap().unwrap().query).await,
        _path => {
            // TODO
            let resp = Response::builder()
                .status(400)
                .header("content-type", "text/html")
                .body("Invalid path".to_string().into())
                .map_err(Box::new)?;
            Ok(resp)
        }
    }
}

async fn serve_frontend() -> Result<Response<Body>, Error> {
    let file = include_str!("./index.html").to_string();

    let resp = Response::builder()
        .status(200)
        .header("content-type", "text/html")
        .body(file.into())
        .map_err(Box::new)?;

    Ok(resp)
}

async fn execute_query(query: &str) -> Result<Response<Body>, Error> {
    println!("Query: {}", query);

    let mut ctx = Context::default();

    let result = ctx.execute_sql(query)?;
    println!("Result: {}", result.print());

    let resp = Response::builder()
        .status(200)
        .header("content-type", "text/html")
        .body(result.print().into())
        .map_err(Box::new)?;

    Ok(resp)
}
