use std::{iter, path::PathBuf, str::FromStr, sync::Arc};
use url::Url;
use uuid::Uuid;

use crate::{
    anyhow, bail, function::JsonMap, ipfs::StorageContent, novi::Novi, Result
};

pub async fn init(novi: &Novi) -> Result<()> {
    novi.register_function(
        "file.url".to_owned(),
        Arc::new(move |(session, store), args: &JsonMap| {
            Box::pin(async move {
                let depth_limit = args.get_u64("depth_limit").unwrap_or(5);
                let id = args.get_id("id")?;
                let variant = args.get_str("variant").unwrap_or("original");

                let object = session.get_object(Some(store.clone()), id).await?;
                let Some(url_str) = object.get_file(variant)? else {
                    bail!(@FileNotFound "null file field");
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
                    _ => url_str.into(),
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

                    let Some(client) = novi.config.ipfs_clients.get(storage) else {
                        bail!(@InvalidArgument "invalid storage")
                    };

                    let object = session.get_object(Some(store.clone()), id).await?;
                    dbg!(variant, &object.tags);
                    if object.get_file(variant).is_ok()
                        && !args.get_bool("overwrite").unwrap_or(false)
                    {
                        bail!(@InvalidState "file already exists");
                    }

                    let content = if let Ok(url) = args.get_str("url") {
                        let resp = reqwest::get(url).await.and_then(|it| it.error_for_status());
                        let resp = match resp {
                            Ok(resp) => resp,
                            Err(err) => bail!(@IOError "failed to download file: {err:?}"),
                        };
                        StorageContent::Response(resp)
                    } else if let Ok(path) = args.get_str("path") {
                        StorageContent::File(PathBuf::from(path))
                    } else {
                        bail!(@InvalidArgument "missing URL or path")
                    };
                    let url = client
                        .put(
                            content,
                            filename,
                        )
                        .await?;
                    let old_identity = session.replace_internal();
                    let result = session
                        .update_object(
                            Some(store.clone()),
                            id,
                            iter::once((format!("@file:{variant}"), Some(url))).collect(),
                            false,
                        )
                        .await;
                    session.replace_identity(old_identity);
                    result?;

                    Ok(JsonMap::default())
                })
            })
        },
        Some("file.store".to_owned()),
    )
    .await?;

    Ok(())
}
