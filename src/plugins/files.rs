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
    // default implementation only handles HTTP, HTTPs and references.
    novi.register_function(
        "file.url".to_owned(),
        Arc::new(move |(session, store), args: &JsonMap| {
            let storage_path = storage_path.clone();
            Box::pin(async move {
                let depth_limit = args.get_u64("depth_limit").unwrap_or(5);
                let prefer_local = args.get_bool("prefer_local").unwrap_or(true);
                let id = args.get_id("id")?;
                let variant = args.get_str("variant").unwrap_or("original");

                let object = session.get_object(Some(store.clone()), id).await?;
                let Some(mut url_str) = object.get(&format!("@file:{variant}")) else {
                    bail!(@FileNotFound)
                };
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
                match url.scheme() {
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
                        session
                            .call_function(store.clone(), "file.url", &args)
                            .await
                    }
                    "http" | "https" => Ok(iter::once(("url".to_owned(), url_str.into())).collect()),
                    scheme => {
                        bail!(@Unsupported "scheme {scheme} is not supported");
                    }
                }
            })
        }),
        None,
    )
    .await?;

    novi.register_function(
        "file.pin".to_owned(),
        Arc::new(move |(session, store), args: &JsonMap| {
            Box::pin(async move {
                let id = args.get_id("id")?;
                let variant = args.get_str("variant")?;

                let args = [
                    ("id".to_owned(), id.to_string().into()),
                    ("variant".to_owned(), variant.to_owned().into()),
                ]
                .into_iter()
                .collect();
                let url = session
                    .call_function(store.clone(), "file.url", &args)
                    .await?;

                todo!()
            })
        }),
        Some("file.pin".to_owned())
    )
    .await?;

    Ok(())
}
