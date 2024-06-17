use std::{borrow::Cow, collections::HashMap, str::FromStr, sync::Arc};
use tokio::sync::RwLock;
use tokio_postgres::types::Type;
use tracing::info;
use uuid::Uuid;

use crate::{
    anyhow,
    filter::{Filter, QueryOptions},
    function::JsonMap,
    hook::{CoreHookArgs, ObjectEdits},
    misc::{now_utc, wrap_nom_from_str},
    novi::Novi,
    object::Object,
    proto::{reg_core_hook_request::HookPoint, SessionMode},
    query::{args_to_ref, QueryBuilder},
    session::Session,
    tag::{TagDict, TagValue},
    Error, Result,
};

mod parse {
    use super::{Action, Consequence, Imply};
    use crate::filter::parse::{filter, string, tag_name};
    use nom::{
        bytes::complete::tag,
        character::complete::{char, multispace0, multispace1},
        combinator::{map, opt},
        multi::separated_list0,
        sequence::{pair, preceded, tuple},
        IResult, Parser,
    };

    pub type Result<'a, R> = IResult<&'a str, R>;

    pub fn consequence(i: &str) -> Result<Consequence> {
        map(
            pair(tag_name, opt(pair(char('='), string))),
            |(tag, set)| {
                let action = match set {
                    Some((_, value)) => Action::Set(Some(value.to_owned())),
                    None => Action::Set(None),
                };
                Consequence {
                    tag: tag.to_owned(),
                    action,
                }
            },
        )
        .or(preceded(char('-'), tag_name).map(|tag| Consequence {
            tag,
            action: Action::Delete,
        }))
        .parse(i)
    }

    pub fn imply(i: &str) -> Result<Imply> {
        map(
            tuple((
                filter,
                multispace0,
                tag("=>"),
                multispace0,
                separated_list0(multispace1, consequence),
            )),
            |(condition, _, _, _, consequences)| Imply {
                condition,
                consequences,
            },
        )(i)
    }
}

pub enum Action {
    Set(Option<String>),
    Delete,
}
impl Action {
    pub fn need_update(&self, current: Option<&TagValue>) -> bool {
        match self {
            Action::Set(value) => current.map_or(true, |it| it.value != *value),
            Action::Delete => current.is_some(),
        }
    }
}

pub struct Consequence {
    tag: String,
    action: Action,
}
impl Consequence {
    pub fn need_update(&self, tags: &TagDict) -> bool {
        self.action.need_update(tags.get(&self.tag))
    }
}

pub struct Imply {
    condition: Filter,
    consequences: Vec<Consequence>,
}
impl FromStr for Imply {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        wrap_nom_from_str(parse::imply(s.trim()))
    }
}

impl Imply {
    pub fn from_object(object: &Object) -> Result<Self> {
        object
            .get("@imply")
            .flatten()
            .ok_or_else(|| anyhow!(@InvalidObject ("id" => object.id.to_string()) "not an imply"))?
            .parse()
    }

    /// Apply the imply to the object and return true if any changes were made.
    pub fn apply(&self, object: &Object, edits: &mut ObjectEdits) -> bool {
        // TODO: Maybe deleted_tags need to be considered as well?
        if !self.condition.matches(object, &Default::default()) {
            return false;
        }

        let mut updated = false;
        for cons in &self.consequences {
            if cons.need_update(&object.tags) {
                updated = true;
                match &cons.action {
                    Action::Set(value) => {
                        edits.set(cons.tag.clone(), value.clone());
                    }
                    Action::Delete => {
                        edits.delete(cons.tag.clone());
                    }
                }
            }
        }

        updated
    }

    /// Build SQL to find objects that need to get updated.
    pub fn build_sql_to_find_unsatified(&self, novi: &Novi, q: &mut QueryBuilder) {
        use std::fmt::Write;

        self.condition.build_sql(q, &novi.internal_identity, None);
        let mut ors = "false".to_owned();
        for cons in &self.consequences {
            let tag = q.bind_string(cons.tag.clone());
            match &cons.action {
                Action::Delete => {
                    write!(ors, " or (tags ? {tag})").unwrap();
                }
                Action::Set(val) => {
                    write!(ors, " or not (tags ? {tag})").unwrap();
                    write!(
                        ors,
                        " or (tags->{tag}->>'v' != {})",
                        q.bind(val.to_owned(), Type::TEXT)
                    )
                    .unwrap();
                }
            }
        }
        q.add_where(ors);
    }
}

pub struct Implies {
    implies: HashMap<Uuid, Imply>,
}
impl Implies {
    fn insert(&mut self, id: Uuid, imply: Imply) {
        self.implies.insert(id, imply);
    }

    /// Apply all implies until no more changes are made. Each imply is applied
    /// at most once.
    pub fn closure(&self, object: &Object, edits: &mut ObjectEdits) -> bool {
        let mut object = Cow::Borrowed(object);

        // A list of implies to apply. This is cheap since it stores references.
        let implies = self.implies.values().collect::<Vec<_>>();

        // We use a linked list to keep track of the unapplied implies.
        let mut next_indices = (1..=implies.len()).collect::<Vec<_>>();
        let mut start_index = 0;

        let mut updated = false;
        loop {
            let mut updated_in_this_round = false;

            let mut last_index = None;
            let mut index = start_index;
            while let Some(imply) = implies.get(index) {
                let mut this_edits = ObjectEdits::new();
                if imply.apply(&object, &mut this_edits) {
                    updated_in_this_round = true;
                    edits.extend(this_edits.clone());
                    let time = object.updated;
                    this_edits.apply(object.to_mut(), time);

                    // Remove the imply from the linked list.
                    if let Some(last_index) = last_index {
                        next_indices[last_index] = next_indices[index];
                    } else {
                        start_index = next_indices[index];
                    }
                }

                last_index = Some(index);
                index = next_indices[index];
            }

            if !updated_in_this_round {
                break;
            } else {
                updated = true;
            }
        }

        updated
    }

    /// Apply an extra imply to the whole database, returning the number of objects affected.
    pub async fn apply_new_imply(&self, session: &mut Session, imply: &Imply) -> Result<usize> {
        let mut q = QueryBuilder::new("object");
        imply.build_sql_to_find_unsatified(&session.novi, &mut q);

        let (sql, args, types) = q.build();
        session.require_mutable()?;

        let mut unsatisfied = Vec::new();
        let stmt = session.connection.prepare_typed(&sql, &types).await?;
        for row in session.connection.query(&stmt, &args_to_ref(&args)).await? {
            unsatisfied.push(Object::from_row(row)?);
        }

        let mut affected = 0;
        for mut object in unsatisfied {
            let time = now_utc();
            let mut edits = ObjectEdits::new();
            if imply.apply(&object, &mut edits) {
                edits.apply(&mut object, time);
                session.save_object(&object).await?;
                affected += 1;
            }
        }

        Ok(affected)
    }
}

async fn add_imply_hook(novi: &Novi, implies: Arc<RwLock<Implies>>) -> Result<()> {
    novi.register_core_hook(HookPoint::BeforeCreate, "~@imply".parse()?, {
        let implies = implies.clone();
        Box::new(move |args: CoreHookArgs| {
            let implies = implies.clone();
            let identity = args.identity().clone();
            let session = args.session.ok().unwrap();
            Box::pin(async move {
                identity.check_perm("imply.create")?;
                let imply = Imply::from_object(args.object)?;
                let mut implies = implies.write().await;
                let result = implies.apply_new_imply(session, &imply).await;
                implies.insert(args.object.id, imply);
                let affected = result?;
                info!(id = %args.object.id, affected, "imply created");

                Ok(ObjectEdits::default())
            })
        })
    })
    .await;
    novi.register_core_hook(
        HookPoint::BeforeUpdate,
        "~@imply".parse()?,
        Box::new(move |args: CoreHookArgs| {
            let implies = implies.clone();
            let identity = args.identity().clone();
            let session = args.session.ok().unwrap();
            Box::pin(async move {
                identity.check_perm("imply.edit")?;
                let imply = Imply::from_object(args.object)?;
                let mut implies = implies.write().await;
                implies.implies.remove(&args.object.id);
                let result = implies.apply_new_imply(session, &imply).await;
                implies.insert(args.object.id, imply);
                let affected = result?;
                info!(id = %args.object.id, affected, "imply updated");

                Ok(ObjectEdits::default())
            })
        }),
    )
    .await;
    Ok(())
}

async fn add_any_hook(novi: &Novi, point: HookPoint, implies: Arc<RwLock<Implies>>) {
    novi.register_core_hook(
        point,
        Filter::all(),
        Box::new(move |args: CoreHookArgs| {
            let implies = implies.clone();
            Box::pin(async move {
                let mut edits = ObjectEdits::new();
                implies.read().await.closure(args.object, &mut edits);
                Ok(edits)
            })
        }),
    )
    .await;
}

async fn add_imply_function(novi: &Novi, implies: Arc<RwLock<Implies>>) -> Result<()> {
    novi.register_function(
        "imply.apply.impl".to_owned(),
        {
            let implies = implies.clone();
            Arc::new(move |session, args: &JsonMap| {
                let implies = implies.clone();
                Box::pin(async move {
                    session.identity.check_perm("imply.apply")?;
                    let imply: Imply = args.get_str("imply")?.parse()?;
                    let affected = implies
                        .read()
                        .await
                        .apply_new_imply(session, &imply)
                        .await?;

                    Ok([("affected".to_owned(), affected.into())]
                        .into_iter()
                        .collect())
                })
            })
        },
        true,
    )
    .await?;
    novi.register_function(
        "imply.apply".to_owned(),
        {
            Arc::new(move |session, args: &JsonMap| {
                Box::pin(async move {
                    session.identity.check_perm("imply.apply")?;
                    session.call_function("imply.apply.impl", args).await
                })
            })
        },
        true,
    )
    .await?;

    Ok(())
}

pub async fn init(novi: &Novi) -> Result<()> {
    let mut session = novi.internal_session(SessionMode::SessionAuto).await?;
    let imply_objs = session
        .query("@imply".parse()?, QueryOptions::default())
        .await?;
    let mut implies = HashMap::new();
    for object in imply_objs {
        implies.insert(object.id, Imply::from_object(&object)?);
    }
    drop(session);

    let implies = Arc::new(RwLock::new(Implies { implies }));

    add_imply_hook(novi, implies.clone()).await?;
    add_any_hook(novi, HookPoint::BeforeCreate, implies.clone()).await;
    add_any_hook(novi, HookPoint::BeforeUpdate, implies.clone()).await;
    add_imply_function(novi, implies.clone()).await?;

    Ok(())
}

#[cfg(test)]
mod test {
    use std::str::FromStr;
    use uuid::Uuid;

    use crate::{hook::ObjectEdits, misc::now_utc, object::Object, tag::TagValue};

    use super::Imply;

    #[test]
    fn test_imply() {
        let time = now_utc();
        let mut object = Object {
            id: Uuid::new_v4(),
            tags: [
                ("test".to_owned(), TagValue::new(None, time)),
                (
                    "test2".to_owned(),
                    TagValue::new(Some("wow".to_owned()), time),
                ),
            ]
            .into_iter()
            .collect(),
            creator: None,
            created: time,
            updated: time,
        };

        let mut edits = ObjectEdits::new();

        let imply = Imply::from_str("no => any").unwrap();
        assert!(!imply.condition.matches(&object, &Default::default()));
        imply.apply(&object, &mut edits);
        assert!(edits.is_empty());

        let imply = Imply::from_str("test => test2=wow").unwrap();
        assert!(imply.condition.matches(&object, &Default::default()));
        imply.apply(&object, &mut edits);
        assert!(edits.is_empty());

        let imply = Imply::from_str("test test2=wow => test3").unwrap();
        assert!(imply.condition.matches(&object, &Default::default()));
        imply.apply(&object, &mut edits);
        assert!(!edits.is_empty());
        edits.apply(&mut object, now_utc());
        assert_eq!(object.tags["test3"].value, None);

        let mut edits = ObjectEdits::new();
        let imply = Imply::from_str("test3/wtf => -test2").unwrap();
        assert!(imply.condition.matches(&object, &Default::default()));
        imply.apply(&object, &mut edits);
        edits.apply(&mut object, now_utc());
        assert!(!object.tags.contains_key("test2"));
    }
}
