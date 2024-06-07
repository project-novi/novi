use chrono::{DateTime, Utc};
use std::collections::{BTreeSet, HashSet};
use tokio_postgres::Row;
use uuid::Uuid;

use crate::{
    anyhow,
    misc::now_utc,
    proto::{self, uuid_to_pb},
    tag::{is_scope, TagDict, TagValue, Tags},
    Result,
};

#[derive(Clone, Debug)]
pub struct Object {
    pub id: Uuid,
    pub tags: TagDict,
    pub creator: Option<Uuid>,
    pub created: DateTime<Utc>,
    pub updated: DateTime<Utc>,
}
impl Object {
    pub(crate) fn from_row(row: Row) -> Result<Self> {
        let id = row.try_get("id")?;
        let tags: serde_json::Value = row.try_get("tags")?;
        let tags = serde_json::from_value(tags).unwrap();

        let creator = row.try_get("creator")?;
        let created = row.try_get("created")?;
        let updated = row.try_get("updated")?;

        Ok(Self {
            id,
            tags,
            creator,
            created,
            updated,
        })
    }

    pub fn get(&self, tag: &str) -> Option<Option<&str>> {
        self.tags.get(tag).map(|it| it.value.as_deref())
    }

    pub fn get_file(&self, variant: &str) -> Result<Option<&str>> {
        self.get(&format!("@file:{variant}"))
            .ok_or_else(|| anyhow!(@FileNotFound "file not found"))
    }

    pub fn remove_tag(&mut self, tag: &str) -> Option<TagValue> {
        self.tags.remove(tag)
    }

    pub fn into_pairs(self) -> impl Iterator<Item = (String, Option<String>)> {
        self.tags.into_iter().map(|(k, v)| (k, v.value))
    }

    pub fn update(&mut self, tags: Tags, force: bool) -> bool {
        let time = now_utc();
        let mut updated = force;
        if force {
            for (tag, value) in tags {
                self.tags.insert(tag, TagValue::new(value, time));
            }
        } else {
            for (tag, value) in tags {
                if self.tags.get(&tag).map_or(true, |it| it.value != value) {
                    self.tags.insert(tag, TagValue::new(value, time));
                    updated = true;
                }
            }
        }
        if updated {
            self.updated = time;
        }
        updated
    }

    pub(crate) fn replace(
        &mut self,
        mut tags: Tags,
        scopes: Option<HashSet<String>>,
        force: bool,
    ) -> Option<BTreeSet<String>> {
        let time = now_utc();
        let mut updated = force;
        let mut deleted_tags = BTreeSet::new();

        self.tags.retain(|tag, value| {
            if let Some(scopes) = &scopes {
                if !scopes.iter().any(|scope| is_scope(tag, scope)) {
                    return true;
                }
            }

            if let Some(new_value) = tags.remove(tag) {
                if force || value.value != new_value {
                    *value = TagValue::new(new_value, time);
                    updated = true;
                }
                true
            } else {
                deleted_tags.insert(tag.to_owned());
                updated = true;
                false
            }
        });

        for (tag, value) in tags {
            self.tags.insert(tag, TagValue::new(value, time));
            updated = true;
        }

        if updated {
            self.updated = time;
            Some(deleted_tags)
        } else {
            None
        }
    }

    pub(crate) fn delete_tags(&mut self, tags: Vec<String>) -> BTreeSet<String> {
        tags.into_iter()
            .filter(|it| self.tags.remove(it).is_some())
            .collect()
    }

    pub fn subtags<'a>(&'a self, scope: &'a str) -> impl Iterator<Item = (&str, &TagValue)> + 'a {
        let start = format!("{scope}:");
        let end = format!("{scope};");
        self.tags
            .range::<String, _>(&start..&end)
            .filter_map(move |(k, v)| k.strip_prefix(&start).map(|it| (it, v)))
    }
}

impl From<Object> for proto::Object {
    fn from(object: Object) -> Self {
        Self {
            id: Some(uuid_to_pb(object.id)),
            tags: object
                .tags
                .into_iter()
                .map(|(k, v)| {
                    (
                        k,
                        proto::TagValue {
                            value: v.value,
                            updated: v.updated.timestamp_micros(),
                        },
                    )
                })
                .collect(),
            creator: object.creator.map(uuid_to_pb),
            created: object.created.timestamp_micros(),
            updated: object.updated.timestamp_micros(),
        }
    }
}

#[cfg(test)]
mod test {
    use uuid::Uuid;

    use crate::misc::now_utc;

    use super::Object;

    #[test]
    fn test_edit() {
        let time = now_utc();
        let mut object = Object {
            id: Uuid::new_v4(),
            tags: Default::default(),
            created: time,
            updated: time,
            creator: None,
        };

        assert!(object.update(
            [("name".into(), Some("test".into()))].into_iter().collect(),
            false
        ));
        assert!(!object.update(
            [("name".into(), Some("test".into()))].into_iter().collect(),
            false
        ));
        assert!(object.update(
            [("name".into(), Some("test".into()))].into_iter().collect(),
            true
        ));

        object.update(
            [
                ("@cat.name".into(), Some("test".into())),
                ("@cat.age".into(), Some("13".into())),
            ]
            .into_iter()
            .collect(),
            false,
        );
        assert!(object
            .replace(
                [
                    ("@cat.name".into(), Some("test".into())),
                    ("@cat.age".into(), Some("13".into())),
                ]
                .into_iter()
                .collect(),
                Some(["@cat".into()].iter().cloned().collect()),
                false,
            )
            .is_none());
        assert_eq!(
            object.replace(
                [("@cat.name".into(), Some("test".into())),]
                    .into_iter()
                    .collect(),
                Some(["@cat".into()].iter().cloned().collect()),
                false,
            ),
            Some(["@cat.age".into()].iter().cloned().collect())
        );

        assert!(object
            .replace(
                [("name".into(), Some("test".into())),]
                    .into_iter()
                    .collect(),
                Some(["".into()].iter().cloned().collect()),
                false,
            )
            .is_none(),);
        assert_eq!(
            object.replace(
                [("name2".into(), Some("test".into())),]
                    .into_iter()
                    .collect(),
                Some(["".into()].iter().cloned().collect()),
                false,
            ),
            Some(["name".into()].iter().cloned().collect())
        );

        assert_eq!(
            object.delete_tags(vec!["name2".into(), "name3".into()]),
            ["name2".into()].iter().cloned().collect()
        );
    }
}
