use std::{
    iter,
    path::{Path, PathBuf},
    str::FromStr,
    sync::Arc,
};
use tracing::warn;
use url::Url;
use uuid::Uuid;

use crate::{
    anyhow, bail,
    filter::Filter,
    function::JsonMap,
    hook::{CoreHookArgs, ObjectEdits},
    novi::Novi,
    proto::reg_core_hook_request::HookPoint,
    storage::{StorageContent, StorageContext},
    Result,
};

fn file_path(storage_path: &Path, id: Uuid, variant: &str) -> PathBuf {
    if variant == "original" {
        storage_path.join(id.to_string())
    } else {
        storage_path.join(format!("{id}.{variant}"))
    }
}

pub async fn init(novi: &Novi) -> Result<()> {
    let storage_path = Arc::new(novi.config.storage_path.clone());
    novi.register_core_hook(HookPoint::AfterDelete, Filter::all(), {
        let storage_path = storage_path.clone();
        Box::new(move |args: CoreHookArgs| {
            let storage_path = storage_path.clone();
            Box::pin(async move {
                for (variant, _) in args.object.subtags("@file") {
                    let path = file_path(&storage_path, args.object.id, variant);
                    if let Err(err) = tokio::fs::remove_file(path).await {
                        warn!(id = %args.object.id, variant, ?err, "failed to delete object file");
                    }
                }
                Ok(ObjectEdits::default())
            })
        })
    })
    .await;

    // This function is intended to be hooked to extend functionality, the
    // default implementation only handles HTTP, HTTPs, IPFS and references.
    let ipfs_gateway = Arc::new(novi.config.ipfs_gateway.clone());
    novi.register_function(
        "file.url".to_owned(),
        Arc::new(move |(session, store), args: &JsonMap| {
            let storage_path = storage_path.clone();
            let ipfs_gateway = Arc::clone(&ipfs_gateway);
            Box::pin(async move {
                let depth_limit = args.get_u64("depth_limit").unwrap_or(5);
                let prefer_local = args.get_bool("prefer_local").unwrap_or(false);
                let id = args.get_id("id")?;
                let variant = args.get_str("variant").unwrap_or("original");

                let object = session.get_object(Some(store.clone()), id).await?;
                let mut url_str = object.get_file(variant)?;
                if prefer_local && file_path(&storage_path, id, variant).exists() {
                    url_str = None;
                }
                let Some(url_str) = url_str else {
                    return Ok(iter::once((
                        "url".to_owned(),
                        format!("file://{id}/{variant}").into(),
                    ))
                    .collect());
                };
                let Ok(url) = Url::parse(url_str) else {
                    bail!(@InvalidArgument "invalid URL")
                };
                let url: serde_json::Value = match url.scheme() {
                    "object" => {
                        if depth_limit == 0 {
                            bail!(@FileNotFound "depth limit exceeded");
                        }
                        let Some(id) = url.host_str().and_then(|it| Uuid::from_str(it).ok()) else {
                            bail!(@InvalidArgument "invalid object ID")
                        };
                        let variant = url.path().strip_prefix('/').unwrap_or("original");
                        let args = [
                            ("depth_limit".to_owned(), (depth_limit - 1).into()),
                            ("id".to_owned(), id.to_string().into()),
                            ("variant".to_owned(), variant.to_owned().into()),
                        ]
                        .into_iter()
                        .collect();
                        return session
                            .call_function(store.clone(), "file.url", &args)
                            .await;
                    }
                    "http" | "https" => url_str.into(),
                    "ipfs" => {
                        let Some(cid) = url.host_str() else {
                            bail!(@InvalidArgument "invalid CID")
                        };
                        let path = url.path();
                        format!("{ipfs_gateway}/ipfs/{cid}{path}").into()
                    }
                    scheme => {
                        bail!(@Unsupported "scheme {scheme} is not supported");
                    }
                };
                Ok(iter::once(("url".to_owned(), url)).collect())
            })
        }),
        None,
    )
    .await?;

    novi.register_function(
        "file.store".to_owned(),
        {
            let novi = novi.clone();
            Arc::new(move |(session, store), args: &JsonMap| {
                let novi = novi.clone();
                Box::pin(async move {
                    let id = args.get_id("id")?;
                    let variant = args.get_str("variant").unwrap_or("original");
                    let storage = args.get_str("storage").unwrap_or("default");
                    let filename = args.get_str("filename").map(str::to_owned).ok();

                    let Some(storage) = novi.config.storages.get(storage) else {
                        bail!(@InvalidArgument "invalid storage")
                    };

                    let object = session.get_object(Some(store.clone()), id).await?;
                    if object.get_file(variant).is_ok()
                        && !args.get_bool("overwrite").unwrap_or(false)
                    {
                        bail!(@InvalidState "file already exists");
                    }

                    let content = if let Ok(url) = args.get_str("url") {
                        if let Some(path) = url.strip_prefix("file://") {
                            StorageContent::File(PathBuf::from(path))
                        } else {
                            let resp = reqwest::get(url).await.and_then(|it| it.error_for_status());
                            let resp = match resp {
                                Ok(resp) => resp,
                                Err(err) => bail!(@IOError "failed to download file: {err:?}"),
                            };
                            StorageContent::Response(resp)
                        }
                    } else if let Ok(path) = args.get_str("path") {
                        StorageContent::File(PathBuf::from(path))
                    } else {
                        bail!(@InvalidArgument "missing URL or path")
                    };
                    let url = storage
                        .put(
                            StorageContext {
                                id,
                                variant,
                                filename: filename.as_deref(),
                            },
                            content,
                        )
                        .await?;
                    session
                        .update_object(
                            Some(store.clone()),
                            id,
                            iter::once((format!("@file:{variant}"), url)).collect(),
                            false,
                        )
                        .await?;

                    Ok(JsonMap::default())
                })
            })
        },
        Some("file.download".to_owned()),
    )
    .await?;

    Ok(())
}
