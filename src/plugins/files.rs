use std::{path::PathBuf, str::FromStr, sync::Arc};
use url::Url;
use uuid::Uuid;

use crate::{anyhow, bail, function::JsonMap, ipfs::StorageContent, novi::Novi, Result};

pub async fn init(novi: &Novi) -> Result<()> {
    novi.register_function(
        "file.url".to_owned(),
        Arc::new(move |session, args: &JsonMap| {
            Box::pin(async move {
                let depth_limit = args.get_u64("depth_limit").unwrap_or(5);
                let id = args.get_id("id")?;
                let variant = args.get_str("variant").unwrap_or("original");
                let allow_invalid = args.get_bool("allow_invalid").unwrap_or_default();

                let to_result = |url: Option<String>| {
                    Ok([
                        ("url".to_owned(), url.into()),
                        ("id".to_owned(), id.to_string().into()),
                        ("variant".to_owned(), variant.to_owned().into()),
                    ]
                    .into_iter()
                    .collect())
                };

                let object = session.get_object(id, false).await?;
                let Ok(Some(url_str)) = object.get_file(variant) else {
                    if allow_invalid {
                        return to_result(None);
                    }
                    bail!(@FileNotFound "empty file field");
                };
                let Ok(url) = Url::parse(url_str) else {
                    if allow_invalid {
                        return to_result(Some(url_str.to_owned()));
                    }
                    bail!(@InvalidArgument "invalid URL")
                };
                if url.scheme() == "object" {
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
                        ("allow_invalid".to_owned(), allow_invalid.into()),
                    ]
                    .into_iter()
                    .collect();
                    session.call_function("file.url", &args).await
                } else {
                    to_result(Some(url_str.to_owned()))
                }
            })
        }),
        true,
    )
    .await?;

    novi.register_function(
        "file.put.impl".to_owned(),
        {
            let novi = novi.clone();
            Arc::new(move |_session, args: &JsonMap| {
                let novi = novi.clone();
                Box::pin(async move {
                    let storage = args.get_str("storage").unwrap_or("default");
                    let filename = args.get_str("filename").map(str::to_owned).ok();

                    let Some(client) = novi.config.ipfs_clients.get(storage) else {
                        bail!(@InvalidArgument "invalid storage")
                    };

                    let content = if let Ok(url) = args.get_str("local") {
                        StorageContent::Local(url.to_owned())
                    } else if let Ok(url) = args.get_str("url") {
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
                    let url = client.put(content, filename).await?;

                    Ok([("url".to_owned(), url.into())].into_iter().collect())
                })
            })
        },
        true,
    )
    .await?;
    novi.register_function(
        "file.put".to_owned(),
        Arc::new(move |session, args: &JsonMap| {
            Box::pin(async move {
                session.identity.check_perm("file.put")?;
                session.call_function("file.put.impl", args).await
            })
        }),
        false,
    )
    .await?;

    novi.register_function(
        "file.store".to_owned(),
        Arc::new(move |session, args: &JsonMap| {
            Box::pin(async move {
                let id = args.get_id("id")?;
                let variant = args.get_str("variant").unwrap_or("original");
                session
                    .identity
                    .check_perm(&format!("file.store:{variant}"))?;

                let object = session.get_object(id, true).await?;
                if object.get_file(variant).is_ok() && !args.get_bool("overwrite").unwrap_or(false)
                {
                    bail!(@InvalidState "file already exists");
                }

                let result = session.call_function("file.put", args).await?;
                let url = result.get_str("url")?;

                let old_identity = session.replace_internal();
                let result = session
                    .update_object(
                        id,
                        [(format!("@file:{variant}"), Some(url.to_owned()))]
                            .into_iter()
                            .collect(),
                        false,
                    )
                    .await;
                session.replace_identity(old_identity);
                result?;

                Ok(JsonMap::default())
            })
        }),
        false,
    )
    .await?;

    Ok(())
}
