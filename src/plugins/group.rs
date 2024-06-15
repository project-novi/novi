use std::sync::Arc;

use tracing::debug;
use uuid::Uuid;

use crate::{
    filter::{Filter, FilterKind, QueryOptions},
    function::JsonMap,
    hook::{CoreHookArgs, ObjectEdits},
    novi::Novi,
    object::Object,
    proto::reg_core_hook_request::HookPoint,
    session::Session,
    Result,
};

async fn get_children(session: &mut Session, id: Uuid) -> Result<Vec<Object>> {
    session
        .query(
            Filter::Atom {
                tag: "@parent".to_owned(),
                kind: FilterKind::Equals(id.to_string(), true),
                prefix: false,
            },
            QueryOptions::default(),
        )
        .await
}

pub async fn init(novi: &Novi) -> Result<()> {
    novi.register_core_hook(
        HookPoint::AfterDelete,
        "@group".parse()?,
        Box::new(move |args: CoreHookArgs| {
            let session = args.session.ok().unwrap();
            Box::pin(async move {
                debug!(id = %args.object.id, "cascade delete objects");
                let children = get_children(session, args.object.id).await?;
                for object in children {
                    session.delete_object(object.id).await?;
                }

                Ok(ObjectEdits::default())
            })
        }),
    )
    .await;
    novi.register_function(
        "group.release".to_owned(),
        Arc::new(|session, args| {
            Box::pin(async move {
                let id = args.get_id("id")?;
                let keep_hidden = args.get_bool("keep_hidden").unwrap_or_default();
                let keep_self = args.get_bool("keep_self").unwrap_or_default();

                let children = get_children(session, id).await?;
                let mut tags_to_delete = vec!["@parent".to_owned()];
                if !keep_hidden {
                    tags_to_delete.push("@hidden".to_owned());
                }
                for object in children {
                    session
                        .delete_object_tags(object.id, tags_to_delete.clone())
                        .await?;
                }
                session
                    .delete_object_tags(id, vec!["@group".to_owned()])
                    .await?;
                if !keep_self {
                    session.delete_object(id).await?;
                }

                Ok(JsonMap::default())
            })
        }),
        true,
    )
    .await?;

    Ok(())
}
