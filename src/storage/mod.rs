mod dir;
mod ipfs;

use std::{path::PathBuf, sync::Arc};
use tonic::async_trait;
use uuid::Uuid;

use crate::{bail, Result};

pub enum StorageContent {
    Response(reqwest::Response),
    File(PathBuf),
}

pub struct StorageContext<'a> {
    pub id: Uuid,
    pub variant: &'a str,
    pub filename: Option<&'a str>,
}

#[async_trait]
pub trait Storage {
    async fn put(&self, context: StorageContext<'async_trait>, content: StorageContent) -> Result<Option<String>>;
}

pub fn parse_spec(spec: &str) -> Result<Arc<dyn Storage + Send + Sync>> {
    let Some((scheme, arg)) = spec.split_once('@') else {
        bail!(@InvalidArgument "missing scheme in storage spec")
    };
    match scheme {
        "ipfs" => {
            let url = arg.to_owned();
            Ok(Arc::new(ipfs::IpfsStorage::new(url)))
        }
        "dir" => {
            let path = PathBuf::from(arg);
            Ok(Arc::new(dir::DirStorage::new(path)))
        }
        _ => bail!(@InvalidArgument "unknown storage scheme: {scheme}"),
    }
}
