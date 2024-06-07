use reqwest::{multipart::{Form, Part}, Client};
use tokio::fs::File;
use std::path::PathBuf;
use serde::Deserialize;

use crate::{anyhow, Result};

pub enum StorageContent {
    Response(reqwest::Response),
    File(PathBuf),
}

#[derive(Deserialize)]
struct AddResponse {
    #[serde(rename = "Hash")]
    hash: String,
}

pub struct IpfsClient {
    url: String,
}
impl IpfsClient {
    pub fn new(url: String) -> Self {
        Self { url }
    }

    pub async fn put(
        &self,
        content: StorageContent,
        filename: Option<String>,
    ) -> Result<String> {
        let mut file_part = match content {
            StorageContent::Response(resp) => Part::stream(resp),
            StorageContent::File(path) => Part::stream(
                File::open(&path)
                    .await
                    .map_err(|_| anyhow!(@IOError "cannot open file: {}", path.display()))?,
            ),
        };
        if let Some(filename) = filename {
            file_part = file_part.file_name(filename);
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

        Ok(format!("ipfs://{}", resp.hash))
    }
}