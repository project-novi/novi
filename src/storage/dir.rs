use std::path::PathBuf;
use tokio::io::AsyncWriteExt;
use tonic::async_trait;

use super::{Storage, StorageContent, StorageContext};
use crate::{anyhow, Result};

pub struct DirStorage {
    path: PathBuf,
}
impl DirStorage {
    pub fn new(path: PathBuf) -> Self {
        let _ = std::fs::create_dir_all(&path);
        Self { path }
    }
}

#[async_trait]
impl Storage for DirStorage {
    async fn put(
        &self,
        context: StorageContext<'async_trait>,
        content: StorageContent,
    ) -> Result<Option<String>> {
        let path = if context.variant == "original" {
            self.path.join(context.id.to_string())
        } else {
            self.path
                .join(format!("{}.{}", context.id, context.variant))
        };
        let mut file = tokio::fs::File::create(&path)
            .await
            .map_err(|_| anyhow!(@IOError "cannot create file: {}", path.display()))?;
        match content {
            StorageContent::Response(mut resp) => {
                let mut buf = vec![];
                while let Some(chunk) = resp
                    .chunk()
                    .await
                    .map_err(|_| anyhow!(@IOError "cannot read response"))?
                {
                    file.write_all(&chunk).await.map_err(
                        |_| anyhow!(@IOError "cannot write to file: {}", path.display()),
                    )?;
                    buf.extend_from_slice(&chunk);
                }
            }
            StorageContent::File(inf_path) => {
                let mut inf = tokio::fs::File::open(&inf_path)
                    .await
                    .map_err(|_| anyhow!(@IOError "cannot open file: {}", inf_path.display()))?;
                tokio::io::copy(&mut inf, &mut file)
                    .await
                    .map_err(|_| anyhow!(@IOError "cannot copy file: {} -> {}", inf_path.display(), path.display()))?;
            }
        }
        Ok(None)
    }
}
