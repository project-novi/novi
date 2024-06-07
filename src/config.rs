use serde::Deserialize;
use std::{collections::HashMap, path::PathBuf, sync::Arc};

use crate::{bail, storage::Storage, Result};

fn default_rpc_address() -> String {
    "unix:novi.socket".to_owned()
}

fn default_ipfs_gateway() -> String {
    "http://127.0.0.1:8080".to_owned()
}

fn default_storage_descs() -> HashMap<String, String> {
    [("default".to_owned(), "dir@storage".to_owned())]
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
    #[serde(rename = "storages", default = "default_storage_descs")]
    storage_descs: HashMap<String, String>,

    #[serde(default)]
    pub guest_permissions: Vec<String>,

    #[serde(skip)]
    pub storages: HashMap<String, Arc<dyn Storage + Send + Sync>>,
}
impl Config {
    pub fn validate(&mut self) -> Result<()> {
        if !self.storage_descs.contains_key("default") {
            bail!(@InvalidArgument "missing default storage")
        }
        for (name, desc) in self.storage_descs.iter() {
            let storage = crate::storage::parse_spec(desc)?;
            self.storages.insert(name.clone(), storage);
        }
        Ok(())
    }
}
