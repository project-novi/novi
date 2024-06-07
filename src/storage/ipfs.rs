use reqwest::{
    multipart::{Form, Part},
    Client,
};
use serde::Deserialize;
use tokio::fs::File;
use tonic::async_trait;

use super::{Storage, StorageContent, StorageContext};
use crate::{anyhow, Result};

pub struct IpfsStorage {
    url: String,
}
impl IpfsStorage {
    pub fn new(url: String) -> Self {
        Self { url }
    }
}

#[derive(Deserialize)]
struct AddResponse {
    #[serde(rename = "Hash")]
    hash: String,
}

#[async_trait]
impl Storage for IpfsStorage {
    async fn put(
        &self,
        context: StorageContext<'async_trait>,
        content: StorageContent,
    ) -> Result<Option<String>> {
        let mut file_part = match content {
            StorageContent::Response(resp) => Part::stream(resp),
            StorageContent::File(path) => Part::stream(
                File::open(&path)
                    .await
                    .map_err(|_| anyhow!(@IOError "cannot open file: {}", path.display()))?,
            ),
        };
        if let Some(filename) = context.filename {
            file_part = file_part.file_name(filename.to_owned());
        }
        let form = Form::new().part("file", file_part);
        let resp = Client::new()
            .post(format!("{}/api/v0/add", self.url))
            .multipart(form)
            .send()
            .await;
        let resp = match resp {
            Ok(resp) => resp.json::<AddResponse>().await,
            Err(err) => Err(err),
        };
        let resp = resp.map_err(|err| anyhow!(@IOError "failed to upload file: {err}"))?;

        Ok(Some(format!("ipfs://{}", resp.hash)))
    }
}
