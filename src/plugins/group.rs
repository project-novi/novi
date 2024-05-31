use tracing::debug;

use crate::{
    filter::{Filter, FilterKind, QueryOptions},
    hook::{HookArgs, ObjectEdits},
    novi::Novi,
    proto::reg_hook_request::HookPoint,
    Result,
};

pub async fn init(novi: &Novi) -> Result<()> {
    novi.register_hook(
        HookPoint::AfterDelete,
        "@group".parse()?,
        Box::new(move |args: HookArgs| {
            let (session, _) = args.session.unwrap();
            Box::pin(async move {
                debug!(id = %args.object.id, "cascade delete objects");
                let children = session
                    .query(
                        None,
                        Filter::Atom {
                            tag: "@parent".to_owned(),
                            kind: FilterKind::Equals(args.object.id.to_string(), true),
                            prefix: false,
                        },
                        QueryOptions::default(),
                    )
                    .await?;
                for object in children {
                    session.delete_object(None, object.id).await?;
                }

                Ok(ObjectEdits::default())
            })
        }),
    )
    .await;
    Ok(())
}
