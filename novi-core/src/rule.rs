use crate::{
    anyhow, misc::wrap_nom_from_str, query::QueryBuilder, Error, ErrorKind, Filter, Model, Object,
    Result, TagValue,
};
use chrono::{DateTime, Utc};
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::{Debug, Display},
    str::FromStr,
};
use uuid::Uuid;

mod parse {
    use super::{Consequence, Consequences, Rule};
    use crate::{anyhow, filter::parse::filter, Filter, FilterKind};
    use nom::{
        bytes::complete::tag, character::complete::multispace0, combinator::map, sequence::tuple,
        IResult,
    };

    pub type Result<'a, R> = IResult<&'a str, R>;

    pub fn consequences(i: &str) -> Result<crate::Result<Consequences>> {
        map(filter, |f| {
            let ops = (|f| {
                let conds = match f {
                    Filter::Atom(..) | Filter::Neg(_) => vec![f],
                    Filter::Ands(conds) => conds,
                    _ => return None,
                };
                conds
                    .into_iter()
                    .map(|f| match f {
                        Filter::Atom(tag, kind) => {
                            let cons = match kind {
                                FilterKind::Has => Consequence::Set(None),
                                FilterKind::Equals(val, true) => Consequence::Set(Some(val)),
                                _ => return None,
                            };
                            Some((tag, cons))
                        }
                        Filter::Neg(f) => match *f {
                            Filter::Atom(tag, FilterKind::Has) => Some((tag, Consequence::Delete)),
                            _ => None,
                        },
                        _ => None,
                    })
                    .collect::<Option<Vec<_>>>()
                    .map(Consequences)
            })(f);
            ops.ok_or_else(|| anyhow!(@InvalidRule "invalid implication consequence"))
        })(i)
    }

    pub fn rule(i: &str) -> Result<crate::Result<Rule>> {
        map(
            tuple((filter, multispace0, tag("=>"), multispace0, consequences)),
            |(conds, _, _, _, cons)| Ok(Rule::new(conds, cons?)),
        )(i)
    }
}

#[derive(Clone)]
pub enum Consequence {
    Set(Option<String>),
    Delete,
}

impl Consequence {
    pub fn need_update(&self, tag_value: Option<&TagValue>) -> bool {
        match self {
            Self::Set(val) => tag_value.map_or(true, |it| &it.value != val),
            Self::Delete => tag_value.is_some(),
        }
    }
}

#[derive(Clone)]
pub struct Consequences(Vec<(String, Consequence)>);
impl FromStr for Consequences {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        wrap_nom_from_str(parse::consequences(s.trim()), ErrorKind::InvalidRule).and_then(|it| it)
    }
}
impl Display for Consequences {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (tag, cons) in &self.0 {
            match cons {
                Consequence::Set(Some(value)) => write!(f, " {tag}={value}")?,
                Consequence::Set(None) => write!(f, " {tag}")?,
                Consequence::Delete => write!(f, " -{tag}")?,
            }
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct Rule {
    id: Uuid,

    condition: Filter,
    consequences: Consequences,

    tags: BTreeMap<String, Option<String>>,
}

impl TryFrom<Object> for Rule {
    type Error = Error;

    fn try_from(value: Object) -> Result<Self> {
        fn inner(mut value: Object) -> Option<Rule> {
            let rule = value.remove_tag("@rule")?.value?;
            Some(Rule {
                id: value.id,
                tags: value.into_pairs().collect(),
                ..rule.parse().ok()?
            })
        }

        let id = value.id();
        inner(value).ok_or_else(|| anyhow!(@InvalidObject "object {id} is not a rule"))
    }
}

impl Model for Rule {
    fn id(&self) -> Uuid {
        self.id
    }

    fn to_tags(&self) -> BTreeMap<String, Option<String>> {
        let mut tags = self.tags.clone();
        tags.insert("@rule".to_owned(), Some(self.to_string()));
        tags
    }
}

impl FromStr for Rule {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        wrap_nom_from_str(parse::rule(s.trim()), ErrorKind::InvalidRule).and_then(|it| it)
    }
}

impl Rule {
    pub fn new(conds: Filter, cons: Consequences) -> Self {
        Self {
            id: Uuid::nil(),

            condition: conds,
            consequences: cons,

            tags: BTreeMap::new(),
        }
    }

    pub fn apply<'a>(&self, object: &mut Object, time: DateTime<Utc>) -> bool {
        let mut updated = false;

        if self.condition.satisfies(object, &BTreeSet::new()) {
            for (tag, cons) in &self.consequences.0 {
                if cons.need_update(object.tags().get(tag)) {
                    match cons {
                        Consequence::Set(value) => {
                            object.insert_tag(tag.to_owned(), value.clone(), time);
                        }
                        Consequence::Delete => {
                            object.remove_tag(tag);
                        }
                    }
                    updated = true;
                }
            }
        }

        updated
    }
}

impl Display for Rule {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} => {}", &self.condition, &self.consequences)
    }
}

impl Debug for Rule {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Rule({self})")
    }
}

#[derive(Default)]
pub struct RuleSet {
    rules: BTreeMap<Uuid, Vec<Rule>>,
}

impl RuleSet {
    pub fn new(rules: BTreeMap<Uuid, Vec<Rule>>) -> Self {
        Self { rules }
    }

    pub fn insert_rule(&mut self, id: Uuid, rules: Vec<Rule>) {
        self.rules.insert(id, rules);
    }

    pub fn delete_rule(&mut self, id: Uuid) -> bool {
        self.rules.remove(&id).is_some()
    }

    pub(crate) fn closure(&self, object: &mut Object, time: DateTime<Utc>) -> bool {
        self.closure_inner(object, time, std::iter::empty())
    }

    // TODO optimize
    pub(crate) fn closure_inner<'a>(
        &'a self,
        object: &mut Object,
        time: DateTime<Utc>,
        additional: impl Iterator<Item = &'a Rule>,
    ) -> bool {
        object.meta.updated = time;

        let mut updated = true;
        let mut any_updated = false;

        // The goal is, every implication runs at most one time.
        // We implement this using a linked list.
        let implications: Vec<&Rule> = self.rules.values().flatten().chain(additional).collect();
        let mut next_idx: Vec<_> = (1..=implications.len()).collect();
        let mut start_idx = 0;

        while updated {
            updated = false;

            let mut last_idx = None;

            let mut idx = start_idx;
            while idx < implications.len() {
                let imply = implications[idx];
                if imply.apply(object, time) {
                    updated = true;
                    if let Some(last) = last_idx {
                        next_idx[last] = next_idx[idx];
                    } else {
                        start_idx = next_idx[idx];
                    }
                }

                last_idx = Some(idx);
                idx = next_idx[idx];
            }

            any_updated |= updated;
        }

        any_updated
    }
}

pub fn parse_rules(s: &str) -> Result<Vec<Rule>> {
    let mut rules = Vec::new();

    for line in s.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        rules.push(line.parse()?);
    }

    Ok(rules)
}

pub(crate) fn query_unsatisfied(rules: &[Rule]) -> QueryBuilder {
    use std::fmt::Write;

    let mut q = QueryBuilder::new("object");
    let mut w = "false".to_owned();
    for rule in rules {
        w += " or (";
        rule.condition.add_wheres_inner(&mut q, &mut w);
        w += " and (false";

        for (tag, cons) in &rule.consequences.0 {
            q.bind(tag.clone());
            match cons {
                Consequence::Set(Some(value)) => {
                    write!(
                        w,
                        " or not tags ? ${0} or tags->${0}->>'v' != ${1}",
                        q.take_place(),
                        q.take_place()
                    )
                    .unwrap();
                    q.bind(value.clone());
                }
                Consequence::Set(None) => {
                    write!(w, " or not tags ? ${}", q.take_place()).unwrap();
                }
                Consequence::Delete => {
                    write!(w, " or tags ? ${}", q.take_place()).unwrap();
                }
            }
        }

        w += "))";
    }
    q.add_where_raw(w);

    q
}
