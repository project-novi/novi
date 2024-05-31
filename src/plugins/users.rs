use argon2::PasswordHash;
use once_cell::sync::Lazy;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::{
    bail,
    hook::{HookArgs, ObjectEdits},
    novi::Novi,
    proto::reg_hook_request::HookPoint,
    user::{User, UserRef},
    Result,
};

pub(crate) static USERS: Lazy<RwLock<HashMap<Uuid, UserRef>>> = Lazy::new(Default::default);

async fn add_hook(novi: &Novi, point: HookPoint) -> Result<()> {
    novi.register_hook(
        point,
        "~@user*".parse()?,
        Box::new(|args: HookArgs| {
            Box::pin(async move {
                println!("{} user updating", args.object.id);
                let new_user = User::try_from(args.object.clone())?;
                new_user.validate()?;
                if let Some(hash) = &new_user.password {
                    if PasswordHash::new(hash).is_err() {
                        bail!(@InvalidArgument "invalid password");
                    }
                }
                let new_user = Arc::new(new_user);

                use std::collections::hash_map::Entry;
                match USERS.write().await.entry(args.object.id) {
                    Entry::Occupied(entry) => {
                        entry.get().swap(new_user);
                    }
                    Entry::Vacant(entry) => {
                        entry.insert(Arc::new(new_user.into()));
                    }
                }

                Ok(ObjectEdits::default())
            })
        }),
    )
    .await;
    Ok(())
}
pub async fn init(novi: &Novi) -> Result<()> {
    add_hook(novi, HookPoint::BeforeCreate).await?;
    add_hook(novi, HookPoint::BeforeUpdate).await?;

    novi.register_hook(
        HookPoint::BeforeView,
        "@user".parse()?,
        Box::new(|args: HookArgs| {
            Box::pin(async move {
                let mut edits = ObjectEdits::default();
                if args.object.tags.contains_key("@user.password") {
                    edits.delete("@user.password".to_owned());
                }
                Ok(edits)
            })
        }),
    )
    .await;

    Ok(())
}
