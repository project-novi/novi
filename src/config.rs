use std::{collections::HashMap, path::PathBuf};

use serde::Deserialize;

use crate::{bail, Result};

fn default_rpc_address() -> String {
    "unix:novi.socket".to_owned()
}

fn default_ipfs_gateway() -> String {
    "http://127.0.0.1:3000".to_owned()
}

fn default_ipfs_apis() -> HashMap<String, String> {
    [("default".to_owned(), "http://127.0.0.1:5001".to_owned())]
        .into_iter()
        .collect()
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
    #[serde(default = "default_ipfs_gateway")]
    pub ipfs_gateway: String,
    #[serde(default = "default_ipfs_apis")]
    pub ipfs_apis: HashMap<String, String>,

    #[serde(default)]
    pub guest_permissions: Vec<String>,
}
impl Config {
    pub fn validate(&self) -> Result<()> {
        if !self.ipfs_apis.contains_key("default") {
            bail!(@InvalidArgument "missing default IPFS API")
        }
        Ok(())
    }
}
