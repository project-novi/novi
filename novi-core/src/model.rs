use crate::{Error, Object};
use sqlx::{
    postgres::PgArguments,
    query::{Map, Query},
    Execute, Postgres,
};
use std::collections::BTreeMap;
use uuid::Uuid;

pub trait Model: TryFrom<Object, Error = Error> {
    fn id(&self) -> Uuid;
    fn to_tags(&self) -> BTreeMap<String, Option<String>>;
}

pub(crate) fn unwrap_mapped<F: Send + 'static>(
    mut map: Map<'_, Postgres, F, PgArguments>,
) -> Query<'_, Postgres, PgArguments> {
    let sql = map.sql();
    if let Some(args) = map.take_arguments() {
        sqlx::query_with(sql, args)
    } else {
        sqlx::query(sql)
    }
}
