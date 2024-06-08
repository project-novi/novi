mod config;
mod error;
mod filter;
mod function;
mod hook;
mod identity;
mod ipfs;
mod misc;
mod novi;
mod object;
mod plugins;
mod proto;
mod query;
mod rpc;
mod session;
mod subscribe;
mod tag;
mod token;
mod user;

pub use error::*;

use tokio::net::UnixListener;
use tonic::transport::Server;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use crate::{config::Config, novi::Novi};

fn init_log() {
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .with(tracing_subscriber::EnvFilter::from_default_env())
        .init();
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_log();
    let config: Config = serde_yaml::from_reader(std::fs::File::open("config.yaml")?)?;
    let server = Novi::new(config).await?;

    let facade = rpc::RpcFacade::new(server);

    let socket_path = "/tmp/novi.socket";
    let _ = std::fs::remove_file(socket_path);
    let uds = UnixListener::bind(socket_path)?;
    Server::builder()
        .add_service(proto::novi_server::NoviServer::with_interceptor(
            facade,
            rpc::interceptor,
        ))
        .serve_with_incoming(tokio_stream::wrappers::UnixListenerStream::new(uds))
        .await?;

    Ok(())
}
