use std::{path::PathBuf, sync::Arc};

use crate::{
    anyhow, bail, function::JsonMap, ipfs::StorageContent, novi::Novi, proto::ObjectLock, Result,
};

pub async fn init(novi: &Novi) -> Result<()> {
    novi.register_function(
        "file.put.impl".to_owned(),
        {
            let novi = novi.clone();
            Arc::new(move |_session, args: &JsonMap| {
                let novi = novi.clone();
                Box::pin(async move {
                    let storage = args.get_str_opt("storage")?.unwrap_or("default");
                    let filename = args.get_str_opt("filename")?.map(str::to_owned);

                    let Some(client) = novi.config.ipfs_clients.get(storage) else {
                        bail!(@InvalidArgument "invalid storage")
                    };

                    let content = if let Some(url) = args.get_str_opt("local")? {
                        StorageContent::Local(url.to_owned())
                    } else if let Some(url) = args.get_str_opt("url")? {
                        let resp = reqwest::get(url).await.and_then(|it| it.error_for_status());
                        let resp = match resp {
                            Ok(resp) => resp,
                            Err(err) => bail!(@IOError "failed to download file: {err:?}"),
                        };
                        StorageContent::Response(resp)
                    } else if let Some(path) = args.get_str_opt("path")? {
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
                let variant = args.get_str_opt("variant")?.unwrap_or("original");
                let lock = args
                    .get_lock_opt("lock")?
                    .unwrap_or(ObjectLock::LockExclusive);
                session
                    .identity
                    .check_perm(&format!("file.store:{variant}"))?;

                let object = session.get_object(id, lock).await?;
                if object.get_file(variant).is_ok()
                    && !args.get_bool_opt("overwrite")?.unwrap_or_default()
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
