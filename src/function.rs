use std::collections::HashMap;

use crate::{misc::BoxFuture, rpc::SessionStore, session::Session, Result};

pub type Function = Box<
    dyn for<'a> Fn(
            (&'a mut Session, SessionStore),
            HashMap<String, Vec<u8>>,
        ) -> BoxFuture<'a, Result<Vec<u8>>>
        + Send
        + Sync,
>;
