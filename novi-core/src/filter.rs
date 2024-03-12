use crate::{
    misc::wrap_nom_from_str,
    query::{pg_pattern_escape, QueryBuilder},
    user::USER,
    Error, ErrorKind, Object, Order, Result, TagValue,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer, Serialize};
use sqlx::postgres::PgArguments;
use std::{
    collections::BTreeSet,
    fmt::{Display, Formatter},
    ops::AddAssign,
    str::FromStr,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FilterKind {
    Has,
    Updated,
    Equals(String, bool),
    Contains(String, bool),
}

impl FilterKind {
    pub fn satisfies(&self, val: Option<&TagValue>, updated: DateTime<Utc>) -> bool {
        let val = match val {
            Some(val) => val,
            None => {
                return matches!(
                    self,
                    FilterKind::Equals(_, false) | FilterKind::Contains(_, false)
                )
            }
        };

        match self {
            FilterKind::Has => true,
            FilterKind::Updated => val.updated == updated,
            FilterKind::Equals(value, eq) => (val.value.as_ref() == Some(value)) == *eq,
            FilterKind::Contains(value, eq) => {
                val.value.as_ref().map_or(false, |it| it.contains(value)) == *eq
            }
        }
    }
}

#[derive(Clone, Debug)]
pub enum Filter {
    Atom(String, FilterKind),
    Ands(Vec<Filter>),
    Ors(Vec<Filter>),
    Neg(Box<Filter>),
}

impl Default for Filter {
    fn default() -> Self {
        Self::all()
    }
}

impl Display for Filter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Atom(tag, kind) => match kind {
                FilterKind::Has => write!(f, "{tag}"),
                FilterKind::Updated => write!(f, "+{tag}"),
                FilterKind::Equals(value, eq) => {
                    write!(f, "{tag}{}={value:?}", if *eq { "" } else { "!" })
                }
                FilterKind::Contains(value, eq) => {
                    write!(f, "{tag}{}~={value}", if *eq { "" } else { "!" })
                }
            },
            Self::Ands(conds) => {
                if conds.is_empty() {
                    write!(f, "*")
                } else {
                    write!(f, "({}", conds[0])?;
                    for cond in &conds[1..] {
                        write!(f, " {}", cond)?;
                    }
                    write!(f, ")")
                }
            }
            Self::Ors(conds) => {
                if conds.is_empty() {
                    write!(f, "!")
                } else {
                    write!(f, "({}", conds[0])?;
                    for cond in &conds[1..] {
                        write!(f, " || {}", cond)?;
                    }
                    write!(f, ")")
                }
            }
            Self::Neg(filter) => write!(f, "-{}", filter),
        }
    }
}

impl Filter {
    pub fn all() -> Self {
        Self::Ands(Vec::new())
    }

    pub fn none() -> Self {
        Self::Ors(Vec::new())
    }

    pub fn is_all(&self) -> bool {
        matches!(self, Self::Ands(conds) if conds.is_empty())
    }

    pub fn prefix_with(&mut self, prefix: &str) {
        match self {
            Self::Atom(tag, _) => {
                tag.insert_str(0, prefix);
            }
            Self::Ands(conds) | Self::Ors(conds) => {
                for cond in conds {
                    cond.prefix_with(prefix);
                }
            }
            Self::Neg(filter) => filter.prefix_with(prefix),
        }
    }

    pub fn satisfies(&self, object: &Object) -> bool {
        match self {
            Self::Atom(tag, kind) => kind.satisfies(object.tags().get(tag), object.meta.updated),
            Self::Ands(conds) => conds.iter().all(|it| it.satisfies(object)),
            Self::Ors(conds) => conds.iter().any(|it| it.satisfies(object)),
            Self::Neg(filter) => !filter.satisfies(object),
        }
    }

    fn visit_tags_inner(&self, f: &mut impl FnMut(&str) -> bool) -> bool {
        match self {
            Self::Atom(tag, _) => f(tag),
            Self::Ands(conds) | Self::Ors(conds) => {
                for cond in conds {
                    if cond.visit_tags_inner(f) {
                        return true;
                    }
                }
                false
            }
            Self::Neg(filter) => filter.visit_tags_inner(f),
        }
    }

    pub fn visit_tags(&self, mut f: impl FnMut(&str) -> bool) -> bool {
        self.visit_tags_inner(&mut f)
    }

    pub fn satisfies_excluding(
        &self,
        object: &Object,
        checkpoint: Option<DateTime<Utc>>,
        deleted_tags: &BTreeSet<String>,
    ) -> bool {
        if !self.satisfies(object) {
            return false;
        }

        if let Some(ckpt) = checkpoint {
            if !self.visit_tags(|tag| {
                object
                    .tags
                    .get(tag)
                    .map_or_else(|| deleted_tags.contains(tag), |it| it.updated > ckpt)
            }) {
                return false;
            }
        }

        true
    }
}

impl AddAssign<Filter> for Filter {
    fn add_assign(&mut self, rhs: Filter) {
        if self.is_all() {
            *self = rhs;
            return;
        }
        if rhs.is_all() {
            return;
        }

        if let Self::Ands(conds) = self {
            if let Self::Ands(rhs_conds) = rhs {
                conds.extend(rhs_conds);
            } else {
                conds.push(rhs);
            }
            return;
        }

        let lhs = std::mem::take(self);
        *self = Filter::Ands(vec![lhs, rhs]);
    }
}

impl FromStr for Filter {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.trim();
        if s.is_empty() {
            return Ok(Filter::all());
        }
        wrap_nom_from_str(parse::filter(s), ErrorKind::InvalidFilter)
    }
}

impl Serialize for Filter {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for Filter {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        Filter::from_str(&s).map_err(serde::de::Error::custom)
    }
}

pub(crate) mod parse {
    use crate::tag::valid_nonspace_tag_char;

    use super::{Filter, FilterKind};
    use nom::{
        branch::alt,
        bytes::complete::{is_not, tag, take_while, take_while1, take_while_m_n},
        character::complete::{char, multispace0, multispace1},
        combinator::{map, map_opt, map_res, opt, recognize, value, verify},
        multi::{fold_many0, separated_list1},
        sequence::{delimited, pair, preceded, tuple},
        IResult, Parser,
    };

    type Result<'a, R> = IResult<&'a str, R>;

    fn parse_unicode(i: &str) -> Result<char> {
        let parse_hex = take_while_m_n(1, 6, |c: char| c.is_ascii_hexdigit());
        let parse_delimited_hex = preceded(char('u'), delimited(char('{'), parse_hex, char('}')));

        let parse_u32 = map_res(parse_delimited_hex, move |hex| u32::from_str_radix(hex, 16));

        map_opt(parse_u32, std::char::from_u32)(i)
    }

    fn parse_escaped_char(i: &str) -> Result<char> {
        preceded(
            char('\\'),
            alt((
                parse_unicode,
                value('\n', char('n')),
                value('\r', char('r')),
                value('\t', char('t')),
                value('\u{08}', char('b')),
                value('\u{0C}', char('f')),
                value('\\', char('\\')),
                value('/', char('/')),
                value('"', char('"')),
            )),
        )(i)
    }

    fn literal(i: &str) -> Result<&str> {
        let not_quote_slash = is_not("\"\\");
        verify(not_quote_slash, |s: &str| !s.is_empty())(i)
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum StringFragment<'a> {
        Literal(&'a str),
        EscapedChar(char),
    }

    fn fragment(i: &str) -> Result<StringFragment> {
        alt((
            map(literal, StringFragment::Literal),
            map(parse_escaped_char, StringFragment::EscapedChar),
        ))(i)
    }

    fn quoted_string(i: &str) -> Result<String> {
        let build_string = fold_many0(fragment, String::new, |mut string, fragment| {
            match fragment {
                StringFragment::Literal(s) => string.push_str(s),
                StringFragment::EscapedChar(c) => string.push(c),
            }
            string
        });

        delimited(char('"'), build_string, char('"'))(i)
    }

    fn plain_string(i: &str) -> Result<String> {
        map(take_while1(|c: char| c.is_alphanumeric()), str::to_owned)(i)
    }

    fn string(i: &str) -> Result<String> {
        quoted_string.or(plain_string).parse(i)
    }

    pub(crate) fn tag_name(i: &str) -> Result<String> {
        map(
            recognize(alt((
                take_while1(valid_nonspace_tag_char),
                preceded(char('@'), take_while(valid_nonspace_tag_char)),
                preceded(char('#'), take_while1(valid_nonspace_tag_char)),
            ))),
            str::to_owned,
        )
        .or(quoted_string)
        .parse(i)
    }

    pub fn atom(i: &str) -> Result<Filter> {
        let all = map(char('*'), |_| Filter::all());
        let none = map(char('!'), |_| Filter::none());
        let infix = map(
            tuple((
                tag_name,
                multispace0,
                opt(char('!')),
                opt(char('~')),
                char('='),
                multispace0,
                string,
            )),
            |(tag, _, not, cs, _, _, value)| {
                let kind = if cs.is_some() {
                    FilterKind::Contains(value, not.is_none())
                } else {
                    FilterKind::Equals(value, not.is_none())
                };
                Filter::Atom(tag.to_owned(), kind)
            },
        );
        let prefix = map(
            pair(tag_name, delimited(char('['), filter, char(']'))),
            |(tag, mut filter)| {
                filter.prefix_with(&tag);
                filter
            },
        );
        let has = map(tag_name, |tag| {
            Filter::Atom(tag.to_owned(), FilterKind::Has)
        });
        let updated = map(preceded(char('+'), tag_name), |tag| {
            Filter::Atom(tag.to_owned(), FilterKind::Updated)
        });
        let neg = map(preceded(char('-'), atom), |f| Filter::Neg(Box::new(f)));
        let parens = delimited(
            preceded(char('('), multispace0),
            filter,
            preceded(multispace0, char(')')),
        );

        alt((all, none, infix, prefix, has, updated, neg, parens)).parse(i)
    }

    pub fn ors(i: &str) -> Result<Filter> {
        map(
            separated_list1(tuple((multispace0, tag("||"), multispace0)), atom),
            |t| {
                if t.len() == 1 {
                    t.into_iter().next().unwrap()
                } else {
                    Filter::Ors(t)
                }
            },
        )
        .parse(i)
    }

    pub fn filter(i: &str) -> Result<Filter> {
        map(separated_list1(multispace1, ors), |t| {
            if t.len() == 1 {
                t.into_iter().next().unwrap()
            } else {
                Filter::Ands(t)
            }
        })
        .parse(i)
    }
}

// TODO more general implementation
pub type TimeRange = (Option<DateTime<Utc>>, Option<DateTime<Utc>>);

impl Filter {
    pub fn query(
        &self,
        checkpoint: Option<DateTime<Utc>>,
        updated_range: TimeRange,
        created_range: TimeRange,
        order: Order,
        limit: Option<u32>,
    ) -> (String, PgArguments) {
        let mut q = QueryBuilder::new("object");
        q.add_select("*");
        self.add_wheres(&mut q, checkpoint, updated_range, created_range);
        q.order(format!(
            "{} {}",
            if order.created { "created" } else { "updated" },
            if order.asc { "asc" } else { "desc" }
        ));
        if let Some(limit) = limit {
            q.limit(limit);
        }

        q.build()
    }

    pub(crate) fn add_wheres_inner(&self, q: &mut QueryBuilder, w: &mut String) {
        use std::fmt::Write;
        match self {
            Self::Atom(tag, kind) => {
                q.bind(tag.clone());
                let t = q.take_place();
                match kind {
                    FilterKind::Has => {
                        write!(w, "(tags ? ${t})").unwrap();
                    }
                    FilterKind::Updated => {
                        write!(w, "(tags->${t}->>'u' = updated::text)").unwrap();
                    }
                    FilterKind::Equals(value, eq) => {
                        q.bind(value.clone());
                        let v = q.take_place();
                        if *eq {
                            write!(w, "(tags->${t}->>'v' = ${v})").unwrap();
                        } else {
                            write!(w, "(tags->${t}->>'v' != ${v} or not tags ? ${t})").unwrap();
                        }
                    }
                    FilterKind::Contains(value, eq) => {
                        q.bind(pg_pattern_escape(&value));
                        let v = q.take_place();
                        if *eq {
                            write!(w, "(tags->${t}->>'v' like '%' || ${v} || '%')").unwrap();
                        } else {
                            write!(
                                w,
                                "(tags->${t}->>'v' not like '%' || ${v} || '%' or not tags ? ${t})"
                            )
                            .unwrap();
                        }
                    }
                }
            }
            Self::Ands(conds) => {
                *w += "(true";
                for cond in conds {
                    *w += " and ";
                    cond.add_wheres_inner(q, w);
                }
                w.push(')');
            }
            Self::Ors(conds) => {
                *w += "(false";
                for cond in conds {
                    *w += " or ";
                    cond.add_wheres_inner(q, w);
                }
                w.push(')');
            }
            Self::Neg(filter) => {
                *w += "not ";
                filter.add_wheres_inner(q, w);
            }
        }
    }

    pub fn add_wheres(
        &self,
        q: &mut QueryBuilder,
        checkpoint: Option<DateTime<Utc>>,
        updated_range: TimeRange,
        created_range: TimeRange,
    ) {
        let mut w = String::new();
        self.add_wheres_inner(q, &mut w);
        q.add_where_raw(w);

        if let Some(ckpt) = checkpoint {
            q.bind(ckpt);
            let t = q.take_place();
            q.add_where_raw(format!("updated > ${t}"));
            let mut w = "(false".to_owned();
            self.visit_tags(|tag| {
                use std::fmt::Write;
                q.bind(tag.to_owned());
                let t = q.take_place();
                write!(w, " or tags->${t}->>'u' > ${t}").unwrap();
                true
            });
            w.push(')');
            q.add_where_raw(w);
        }

        let mut add_range = |field: &str, range: TimeRange| {
            if let Some(lower) = range.0 {
                q.add_where(format!("{field} >= ??")).bind(lower);
            }
            if let Some(upper) = range.1 {
                q.add_where(format!("{field} <= ??")).bind(upper);
            }
        };
        add_range("updated", updated_range);
        add_range("created", created_range);

        USER.with(|user| {
            if !user.is_internal() {
                use std::fmt::Write;
                q.bind(user.perms.iter().cloned().collect::<Vec<_>>());
                let mut s = format!("${} @> access_perms", q.take_place());
                if let Some(id) = user.id {
                    q.bind(id);
                    let id = q.take_place();
                    write!(s, " or creator = ${id}").unwrap();
                }
                q.add_where(s);
            }
        });
    }
}
