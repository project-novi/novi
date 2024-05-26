use std::{
    path::{Path, PathBuf},
    sync::Arc,
};
use tracing::warn;
use uuid::Uuid;

use crate::{
    filter::Filter,
    hook::{HookArgs, ObjectEdits},
    novi::Novi,
    proto::reg_hook_request::HookPoint,
};

fn file_path(storage_path: &Path, id: Uuid, variant: &str) -> PathBuf {
    if variant == "original" {
        storage_path.join(id.to_string())
    } else {
        storage_path.join(format!("{id}.{variant}"))
    }
}

pub async fn init(novi: &Novi) {
    let storage_path = Arc::new(novi.config.storage_path.clone());
    novi.register_hook(
        HookPoint::AfterDelete,
        Filter::all(),
        Box::new(move |args: HookArgs| {
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
        }),
    )
    .await;
}
