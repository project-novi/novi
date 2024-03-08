use crate::{model::unwrap_mapped, Result, TagValue};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::{
    postgres::{PgArguments, PgRow},
    query::Query,
    Postgres, Row,
};
use std::collections::BTreeMap;
use uuid::Uuid;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ObjectMeta {
    pub creator: Option<Uuid>,

    pub created: DateTime<Utc>,
    pub updated: DateTime<Utc>,
}

impl ObjectMeta {
    pub(crate) fn from_row(row: PgRow) -> Result<Self> {
        let creator = row.try_get("creator")?;

        let created = row.try_get("created")?;
        let updated = row.try_get("updated")?;

        Ok(Self {
            creator,

            created,
            updated,
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Object {
    pub(crate) id: Uuid,
    pub(crate) tags: BTreeMap<String, TagValue>,

    #[serde(flatten)]
    pub(crate) meta: ObjectMeta,
}

impl Object {
    pub fn id(&self) -> Uuid {
        self.id
    }

    pub(crate) fn query_id(id: Uuid) -> Query<'static, Postgres, PgArguments> {
        unwrap_mapped(sqlx::query!("select * from object where id = $1", id))
    }

    pub(crate) fn from_row(row: PgRow) -> Result<Self> {
        let id = row.try_get("id")?;
        let tags: Value = row.try_get("tags")?;
        let tags = serde_json::from_value(tags).unwrap();

        let meta = ObjectMeta::from_row(row)?;

        Ok(Self { id, tags, meta })
    }
}

impl Object {
    // TODO optimize
    pub(crate) fn save(&self) -> Query<'static, Postgres, PgArguments> {
        sqlx::query!(
            "update object set tags = $2, updated = $3, created = $4 where id = $1",
            self.id,
            serde_json::to_value(&self.tags).unwrap(),
            self.meta.updated,
            self.meta.created
        )
    }

    pub fn tags(&self) -> &BTreeMap<String, TagValue> {
        &self.tags
    }

    pub fn into_tags(self) -> BTreeMap<String, TagValue> {
        self.tags
    }

    pub fn get_tag_value(&self, tag: &str) -> Option<&TagValue> {
        self.tags.get(tag)
    }

    pub fn get(&self, tag: &str) -> Option<&str> {
        self.tags.get(tag).and_then(|it| it.value.as_deref())
    }

    pub fn remove_tag(&mut self, tag: &str) -> Option<TagValue> {
        self.tags.remove(tag)
    }

    pub fn insert_tag(&mut self, tag: String, value: Option<String>, time: DateTime<Utc>) {
        let entry = self.tags.entry(tag).or_insert_with(|| TagValue {
            value: None,
            updated: time,
        });
        entry.value = value;
        entry.updated = time;
    }

    pub fn meta(&self) -> &ObjectMeta {
        &self.meta
    }

    pub fn into_pairs(self) -> impl Iterator<Item = (String, Option<String>)> {
        self.tags.into_iter().map(|(tag, val)| (tag, val.value))
    }
}
