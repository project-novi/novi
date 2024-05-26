use std::path::PathBuf;

use serde::Deserialize;

fn default_rpc_address() -> String {
    "unix:novi.socket".to_owned()
}

fn default_storage_path() -> PathBuf {
    PathBuf::from("storage")
}

#[derive(Deserialize)]
pub struct Config {
    pub database: deadpool_postgres::Config,
    pub redis: deadpool_redis::Config,

    #[serde(default = "default_storage_path")]
    pub storage_path: PathBuf,

    #[serde(default)]
    pub master_key: Option<String>,
    #[serde(default = "default_rpc_address")]
    pub rpc_address: String,
    #[serde(default)]
    pub guest_permissions: Vec<String>,
}
