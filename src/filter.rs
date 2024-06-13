use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer, Serialize};
use std::{collections::BTreeSet, fmt, ops::AddAssign, str::FromStr};
use tokio_postgres::types::Type;

use crate::{
    bail,
    identity::Identity,
    misc::wrap_nom_from_str,
    object::Object,
    proto::query_request::Order,
    query::{pg_pattern_escape, PgArguments, QueryBuilder},
    tag, Error, Result,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FilterKind {
    Has,
    Updated,
    Deleted,
    Modified,
    Equals(String, bool),
    Contains(String, bool),
}

impl FilterKind {
    pub fn satisfies(
        &self,
        object: &Object,
        tag: &str,
        prefix: bool,
        deleted_tags: &BTreeSet<String>,
    ) -> bool {
        let (latest_updated, deleted) = if prefix {
            let (s1, e1) = (format!("{tag}:"), format!("{tag};"));
            let (s2, e2) = (format!("{tag}."), format!("{tag}/"));
            let updated = object
                .tags
                .range::<String, _>(&s1..&e1)
                .chain(object.tags.range::<String, _>(&s2..&e2))
                .map(|(_, v)| v.updated)
                .max();
            let deleted = deleted_tags
                .range::<String, _>(&s1..&e1)
                .chain(deleted_tags.range::<String, _>(&s2..&e2))
                .next()
                .is_some();

            (updated, deleted)
        } else {
            (
                object.tags.get(tag).map(|it| it.updated),
                deleted_tags.contains(tag),
            )
        };
        match self {
            FilterKind::Has => return latest_updated.is_some(),
            FilterKind::Updated => return latest_updated == Some(object.updated),
            FilterKind::Modified => return latest_updated == Some(object.updated) || deleted,
            FilterKind::Deleted => return deleted,
            _ => {}
        }

        assert!(!prefix);
        let val = match object.tags.get(tag) {
            Some(val) => val,
            None => {
                return match self {
                    FilterKind::Equals(_, false) | FilterKind::Contains(_, false) => true,
                    _ => false,
                }
            }
        };
        match self {
            FilterKind::Equals(value, eq) => (val.value.as_ref() == Some(value)) == *eq,
            FilterKind::Contains(value, eq) => {
                val.value.as_ref().map_or(false, |it| it.contains(value)) == *eq
            }
            _ => unreachable!(),
        }
    }
}

#[derive(Clone, Debug)]
pub enum Filter {
    Atom {
        tag: String,
        kind: FilterKind,
        prefix: bool,
    },
    Ands(Vec<Filter>),
    Ors(Vec<Filter>),
    Neg(Box<Filter>),
}

impl Default for Filter {
    fn default() -> Self {
        Self::all()
    }
}

impl fmt::Display for Filter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use fmt::Write;
        match self {
            Self::Atom { tag, kind, prefix } => {
                (match kind {
                    FilterKind::Has => write!(f, "{tag}"),
                    FilterKind::Updated => write!(f, "+{tag}"),
                    FilterKind::Deleted => write!(f, "!{tag}"),
                    FilterKind::Modified => write!(f, "~{tag}"),
                    FilterKind::Equals(value, eq) => {
                        write!(f, "{tag}{}={value:?}", if *eq { "" } else { "!" })
                    }
                    FilterKind::Contains(value, eq) => {
                        write!(f, "{tag}{}%{value}", if *eq { "" } else { "!" })
                    }
                })?;
                if *prefix {
                    f.write_char('*')?;
                }
                Ok(())
            }
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
                    write!(f, "-*")
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

    pub fn is_all(&self) -> bool {
        matches!(self, Self::Ands(conds) if conds.is_empty())
    }

    pub fn prefix_with(&mut self, prefix: &str) {
        match self {
            Self::Atom { tag, .. } => {
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

    pub fn matches(&self, object: &Object, deleted_tags: &BTreeSet<String>) -> bool {
        match self {
            Self::Atom { tag, kind, prefix } => kind.satisfies(object, tag, *prefix, deleted_tags),
            Self::Ands(conds) => conds.iter().all(|it| it.matches(object, deleted_tags)),
            Self::Ors(conds) => conds.iter().any(|it| it.matches(object, deleted_tags)),
            Self::Neg(filter) => !filter.matches(object, deleted_tags),
        }
    }

    pub fn validate(&self) -> Result<()> {
        match self {
            Self::Atom { tag, kind, prefix } => {
                tag::validate_tag_name(tag)?;
                if let FilterKind::Equals(value, _) | FilterKind::Contains(value, _) = kind {
                    if *prefix {
                        bail!(@InvalidArgument "cannot use value query with tag prefix");
                    }
                    tag::validate_tag_value(tag, Some(value))?;
                }
            }
            Self::Ands(conds) | Self::Ors(conds) => {
                for cond in conds {
                    cond.validate()?;
                }
            }
            Self::Neg(filter) => filter.validate()?,
        }
        Ok(())
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
        let result = wrap_nom_from_str(parse::filter(s))?;
        result.validate()?;
        Ok(result)
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
        bytes::complete::{is_not, take_while, take_while1, take_while_m_n},
        character::complete::{char, multispace0, multispace1, one_of},
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

    pub(crate) fn string(i: &str) -> Result<String> {
        quoted_string.or(plain_string).parse(i)
    }

    pub(crate) fn tag_name(i: &str) -> Result<String> {
        map(
            recognize(alt((
                take_while1(valid_nonspace_tag_char),
                preceded(one_of("@#"), take_while(valid_nonspace_tag_char)),
            ))),
            str::to_owned,
        )
        .or(quoted_string)
        .parse(i)
    }

    pub fn atom(i: &str) -> Result<Filter> {
        let all = map(char('*'), |_| Filter::all());
        let infix = map(
            tuple((
                tag_name,
                multispace0,
                opt(char('!')),
                one_of("%="),
                multispace0,
                string,
            )),
            |(tag, _, not, cs, _, value)| {
                let kind = if cs == '%' {
                    FilterKind::Contains(value, not.is_none())
                } else {
                    FilterKind::Equals(value, not.is_none())
                };
                Filter::Atom {
                    tag: tag.to_owned(),
                    kind,
                    prefix: false,
                }
            },
        );
        let prefix = map(
            pair(tag_name, delimited(char('['), filter, char(']'))),
            |(tag, mut filter)| {
                filter.prefix_with(&tag);
                filter
            },
        );
        let unary = map(
            tuple((opt(one_of("+!~")), tag_name, opt(char('*')))),
            |(op, tag, star)| Filter::Atom {
                tag: tag.to_owned(),
                kind: match op {
                    Some('+') => FilterKind::Updated,
                    Some('!') => FilterKind::Deleted,
                    Some('~') => FilterKind::Modified,
                    _ => FilterKind::Has,
                },
                prefix: star.is_some(),
            },
        );
        let neg = map(preceded(char('-'), atom), |f| Filter::Neg(Box::new(f)));
        let parens = delimited(
            preceded(char('('), multispace0),
            filter,
            preceded(multispace0, char(')')),
        );

        alt((all, infix, prefix, unary, neg, parens)).parse(i)
    }

    pub fn ors(i: &str) -> Result<Filter> {
        map(
            separated_list1(tuple((multispace0, char('/'), multispace0)), atom),
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

// TODO: Support more general implementation
pub type TimeRange = (Option<DateTime<Utc>>, Option<DateTime<Utc>>);

pub struct QueryOptions {
    pub checkpoint: Option<DateTime<Utc>>,
    pub created_range: TimeRange,
    pub updated_range: TimeRange,
    pub order: Order,
    pub limit: Option<u32>,
    pub lock: bool,
}
impl Default for QueryOptions {
    fn default() -> Self {
        Self {
            checkpoint: None,
            created_range: TimeRange::default(),
            updated_range: TimeRange::default(),
            order: Order::CreatedDesc,
            limit: None,
            lock: true,
        }
    }
}

impl Filter {
    pub fn query(
        &self,
        identity: &Identity,
        options: QueryOptions,
    ) -> (String, PgArguments, Vec<Type>) {
        let mut q = QueryBuilder::new("object");
        self.build_sql(&mut q, identity, options.checkpoint);

        q.order = Some(
            match options.order {
                Order::CreatedDesc => "created desc",
                Order::CreatedAsc => "created asc",
                Order::UpdatedDesc => "updated desc",
                Order::UpdatedAsc => "updated asc",
            }
            .into(),
        );
        let mut add_range = |field: &str, range: TimeRange| {
            if let Some(lower) = range.0 {
                let clause = format!("{field} >= {}", q.bind(lower, Type::TIMESTAMPTZ));
                q.add_where(clause);
            }
            if let Some(upper) = range.1 {
                let clause = format!("{field} <= {}", q.bind(upper, Type::TIMESTAMPTZ));
                q.add_where(clause);
            }
        };
        add_range("created", options.created_range);
        add_range("updated", options.updated_range);

        q.limit = options.limit;
        q.lock = options.lock;

        q.build()
    }

    pub(crate) fn add_wheres_inner(
        &self,
        q: &mut QueryBuilder,
        w: &mut String,
        ckpt: Option<&str>,
    ) -> Result<()> {
        use std::fmt::Write;
        match self {
            Self::Atom { tag, kind, prefix } => {
                if *prefix {
                    bail!(@InvalidArgument "cannot preform batched prefix query. it's intended to be used to filter events");
                }
                let tag = q.bind(tag.clone(), Type::TEXT);
                match kind {
                    FilterKind::Has => {
                        write!(w, "(tags ? {tag})").unwrap();
                    }
                    FilterKind::Updated | FilterKind::Modified => match ckpt {
                        Some(ckpt) => {
                            // TODO: is this appropriate?
                            write!(w, "((tags->{tag}->>'u')::timestamptz > {ckpt})").unwrap();
                        }
                        None => {
                            write!(w, "((tags->{tag}->>'u')::timestamptz = updated)").unwrap();
                        }
                    },
                    FilterKind::Deleted => {
                        *w += "false";
                    }
                    FilterKind::Equals(value, eq) => {
                        let v = q.bind_string(value.clone());
                        if *eq {
                            write!(w, "(tags->{tag}->>'v' = {v})").unwrap();
                        } else {
                            write!(w, "(tags->{tag}->>'v' != {v} or not tags ? {tag})").unwrap();
                        }
                    }
                    FilterKind::Contains(value, eq) => {
                        let v = q.bind_string(pg_pattern_escape(value));
                        if *eq {
                            write!(w, "(tags->{tag}->>'v' like '%' || {v} || '%')").unwrap();
                        } else {
                            write!(
                                w,
                                "(tags->{tag}->>'v' not like '%' || {v} || '%' or not tags ? {tag})"
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
                    cond.add_wheres_inner(q, w, ckpt)?;
                }
                w.push(')');
            }
            Self::Ors(conds) => {
                *w += "(false";
                for cond in conds {
                    *w += " or ";
                    cond.add_wheres_inner(q, w, ckpt)?;
                }
                w.push(')');
            }
            Self::Neg(filter) => {
                *w += "not ";
                filter.add_wheres_inner(q, w, ckpt)?;
            }
        }
        Ok(())
    }

    pub fn build_sql(
        &self,
        q: &mut QueryBuilder,
        identity: &Identity,
        checkpoint: Option<DateTime<Utc>>,
    ) {
        let mut w = String::new();
        let checkpoint = checkpoint.map(|it| q.bind(it, Type::TIMESTAMPTZ));
        self.add_wheres_inner(q, &mut w, checkpoint.as_deref())
            .unwrap();
        q.add_where(w);

        if !identity.is_admin() {
            use std::fmt::Write;
            let perms = q.bind(identity.permissions(), Type::TEXT_ARRAY);
            let mut s = format!("{perms} @> access_perms");
            if let Some(id) = identity.user.load().id {
                write!(s, " or creator = {}", q.bind(id, Type::UUID)).unwrap();
            }
            q.add_where(s);
        }
    }
}

#[cfg(test)]
mod test {
    use super::Filter;

    #[test]
    fn test_basic_filters() {
        for test in [
            "cat dog",
            "cat/dog",
            "-cat dog",
            "-(cat/dog) tiger",
            "!cat ~tiger +\"big banana\"",
            "@name=\"line1\\nline2\"",
            "@name % test",
            "@name != test",
            "@[name tag]",
        ] {
            test.parse::<Filter>().unwrap();
        }
    }
}
