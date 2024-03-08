use sqlx::{postgres::PgArguments, Arguments, Encode, Postgres, Type};
use std::fmt::Write;

pub fn pg_pattern_escape(s: &str) -> String {
    s.replace('%', "\\%").replace('_', "\\_")
}

pub struct QueryBuilder {
    table: &'static str,
    selects: Vec<String>,
    wheres: Vec<String>,
    pub order: Option<String>,
    limit: Option<u32>,
    offset: Option<u32>,
    args: PgArguments,

    placeholder: u16,
}

impl QueryBuilder {
    pub fn new(table: &'static str) -> Self {
        Self {
            table,
            selects: Vec::new(),
            wheres: Vec::new(),
            limit: None,
            offset: None,
            order: None,
            args: PgArguments::default(),

            placeholder: 0,
        }
    }

    pub fn take_place(&mut self) -> u16 {
        self.placeholder += 1;
        self.placeholder
    }

    fn parse(&mut self, s: &str) -> String {
        let mut res = String::new();
        let mut it = s.split("??");
        if let Some(first) = it.next() {
            res.push_str(first);
            for part in it {
                self.placeholder += 1;
                write!(res, "${}", self.placeholder).unwrap();
                res.push_str(part);
            }
        }
        res
    }

    pub fn add_select(&mut self, field: &str) -> &mut Self {
        let field = self.parse(field);
        self.selects.push(field);
        self
    }

    pub fn add_where_raw(&mut self, clause: impl Into<String>) -> &mut Self {
        self.wheres.push(clause.into());
        self
    }

    pub fn add_where(&mut self, clause: impl AsRef<str>) -> &mut Self {
        let clause = self.parse(clause.as_ref());
        self.wheres.push(clause);
        self
    }

    pub fn bind<
        T: std::fmt::Debug + Send + Encode<'static, Postgres> + Type<Postgres> + 'static,
    >(
        &mut self,
        value: T,
    ) -> &mut Self {
        self.args.add(value);
        self
    }

    pub fn order(&mut self, order: impl Into<String>) -> &mut Self {
        self.order = Some(order.into());
        self
    }

    pub fn limit(&mut self, limit: u32) -> &mut Self {
        self.limit = Some(limit);
        self
    }

    pub fn offset(&mut self, offset: u32) -> &mut Self {
        self.offset = Some(offset);
        self
    }

    fn build_stmt(&self) -> String {
        let mut res = "select ".to_owned();
        assert!(!self.selects.is_empty());

        res.push_str(&self.selects[0]);
        for select in &self.selects[1..] {
            res.push(',');
            res.push_str(select);
        }
        res.push_str(" from ");
        res += self.table;
        let mut it = self.wheres.iter();
        if let Some(first) = it.next() {
            res += " where (";
            res += first;
            for rest in it {
                res += ") and (";
                res += rest;
            }
            res.push(')');
        }

        if let Some(order) = &self.order {
            res += " order by ";
            res += order;
        }
        if let Some(limit) = &self.limit {
            write!(res, " limit {limit}").unwrap();
        }
        if let Some(offset) = &self.offset {
            write!(res, " offset {offset}").unwrap();
        }

        res
    }

    pub fn build(self) -> (String, PgArguments) {
        (self.build_stmt(), self.args)
    }
}
