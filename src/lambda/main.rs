mod html_formatter;

use anyhow::Result;
use html_formatter::{format_error, format_result};
use idk::context::Context;
use lambda_http::{run, service_fn, Body, Error, Request, RequestPayloadExt, Response};
use lambda_runtime::diagnostic::Diagnostic;
use serde::{Deserialize, Serialize};

// concat the css into a single string in compile time
const CSS: &str = concat!(
    include_str!("./views/style.css"),
    include_str!("./views/style2.css"),
);

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
    let path = event.uri().path();

    match path {
        "/" => serve_frontend().await,
        "/css/" => serve_css().await,
        "/query" => execute_query(&event.json::<Item>().unwrap().unwrap().query).await,
        _ => {
            let resp = Response::builder()
                .status(400)
                .header("content-type", "text/html")
                .body("Invalid path".to_string().into())
                .map_err(Box::new)?;
            Ok(resp)
        }
    }
}

async fn serve_css() -> Result<Response<Body>, Error> {
    let resp = Response::builder()
        .status(200)
        .header("content-type", "text/css")
        .body(CSS.into())
        .map_err(Box::new)?;

    Ok(resp)
}

async fn serve_frontend() -> Result<Response<Body>, Error> {
    let file = include_str!("./views/index.html").to_string();

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

    let html = match ctx.execute_sql(query) {
        Ok(result) => format_result(result),
        Err(err) => format_error(err.to_string()),
    };

    let resp = Response::builder()
        .status(200)
        .header("content-type", "text/html")
        .body(html.into())
        .map_err(Box::new)?;

    Ok(resp)
}
