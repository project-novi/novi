use crate::{anyhow, Error, Model, Object, Result};
use chrono::{DateTime, Utc};
use pyo3::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use uuid::Uuid;

pub fn valid_nonspace_tag_char(c: char) -> bool {
    c.is_alphanumeric() || c == '_' || c == ':' || c == '·' || c == '.' || c == '：'
}

pub fn valid_tag_char(c: char) -> bool {
    valid_nonspace_tag_char(c) || c == ' '
}

pub fn scope_of(tag: &str) -> &str {
    if tag.starts_with('@') {
        tag.split_once('.').map_or(tag, |(pre, _)| pre)
    } else {
        ""
    }
}

#[pyclass(module = "novi")]
#[derive(Deserialize, Serialize, Clone, Debug, PartialEq, Eq)]
pub struct TagValue {
    #[serde(rename = "v")]
    #[pyo3(get)]
    pub value: Option<String>,
    #[serde(rename = "u")]
    #[pyo3(get)]
    pub updated: DateTime<Utc>,
}

impl TagValue {
    pub fn create(value: Option<String>, time: DateTime<Utc>) -> Self {
        Self {
            value,
            updated: time,
        }
    }
}

#[derive(Serialize)]
pub struct Tag {
    pub id: Uuid,
    pub name: String,

    pub implies: String,

    #[serde(skip)]
    pub tags: BTreeMap<String, Option<String>>,
}

impl Tag {
    pub fn new(name: String) -> Self {
        Self {
            id: Uuid::nil(),
            name,

            implies: String::new(),

            tags: BTreeMap::new(),
        }
    }
}

impl Model for Tag {
    fn id(&self) -> Uuid {
        self.id
    }

    fn to_tags(&self) -> BTreeMap<String, Option<String>> {
        let mut tags = self.tags.clone();
        tags.insert("@tag".to_owned(), Some(self.name.clone()));
        tags.insert("@tag.implies".to_owned(), Some(self.implies.clone()));

        tags
    }
}

impl TryFrom<Object> for Tag {
    type Error = Error;

    fn try_from(value: Object) -> Result<Self> {
        fn inner(mut value: Object) -> Option<Tag> {
            let name = value.remove_tag("@tag")?.value?;
            let implies = value.remove_tag("@tag.implies")?.value.unwrap_or_default();

            Some(Tag {
                id: value.id,
                name,

                implies,

                tags: value.into_pairs().collect(),
            })
        }

        let id = value.id;
        inner(value).ok_or_else(|| anyhow!(@InvalidObject "object {id} is not a tag"))
    }
}
