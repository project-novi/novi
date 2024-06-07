use serde::Deserialize;
use std::collections::HashMap;

use crate::{bail, ipfs::IpfsClient, Result};

fn default_rpc_address() -> String {
    "unix:novi.socket".to_owned()
}

fn default_ipfs_apis() -> HashMap<String, String> {
    [("default".to_owned(), "http://127.0.0.1:5001".to_owned())]
        .into_iter()
        .collect()
}

#[derive(Deserialize)]
pub struct Config {
    pub database: deadpool_postgres::Config,
    pub redis: deadpool_redis::Config,

    #[serde(default)]
    pub master_key: Option<String>,
    #[serde(default = "default_rpc_address")]
    pub rpc_address: String,
    #[serde(default = "default_ipfs_apis")]
    storages: HashMap<String, String>,

    #[serde(default)]
    pub guest_permissions: Vec<String>,

    #[serde(skip)]
    pub ipfs_clients: HashMap<String, IpfsClient>,
}
impl Config {
    pub fn validate(&mut self) -> Result<()> {
        if !self.storages.contains_key("default") {
            bail!(@InvalidArgument "missing default storage")
        }
        for (name, url) in self.storages.iter() {
            self.ipfs_clients
                .insert(name.clone(), IpfsClient::new(url.clone()));
        }
        Ok(())
    }
}
