use tracing::debug;

use crate::{
    filter::{Filter, FilterKind, QueryOptions},
    hook::{CoreHookArgs, ObjectEdits},
    novi::Novi,
    proto::reg_core_hook_request::HookPoint,
    Result,
};

pub async fn init(novi: &Novi) -> Result<()> {
    novi.register_core_hook(
        HookPoint::AfterDelete,
        "@group".parse()?,
        Box::new(move |args: CoreHookArgs| {
            let session = args.session.ok().unwrap();
            Box::pin(async move {
                debug!(id = %args.object.id, "cascade delete objects");
                let children = session
                    .query(
                        Filter::Atom {
                            tag: "@parent".to_owned(),
                            kind: FilterKind::Equals(args.object.id.to_string(), true),
                            prefix: false,
                        },
                        QueryOptions::default(),
                    )
                    .await?;
                for object in children {
                    session.delete_object(object.id).await?;
                }

                Ok(ObjectEdits::default())
            })
        }),
    )
    .await;
    Ok(())
}
