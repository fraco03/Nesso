use axum::serve;
use std::env;
use std::path::PathBuf;
use tokio::net::TcpListener;

mod server;
mod storage;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut data_dir = env::current_dir()?;
    data_dir.push("nesso_data");

    let app = server::create_router(data_dir);
    let listener = TcpListener::bind("127.0.0.1:8080").await?;
    println!("Server running on 127.0.0.1:8080");
    serve(listener, app).await?;
    Ok(())
}
