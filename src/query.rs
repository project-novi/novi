use std::fmt::Write;
use tokio_postgres::types::{ToSql, Type};

use crate::proto::ObjectLock;

pub type PgArguments = Vec<Box<dyn ToSql + Send + Sync>>;

pub fn pg_pattern_escape(s: &str) -> String {
    s.replace('%', "\\%").replace('_', "\\_")
}

pub struct QueryBuilder {
    pub table: &'static str,
    pub selects: String,
    pub wheres: Vec<String>,
    pub order: Option<String>,
    pub limit: Option<u32>,
    pub offset: Option<u32>,
    pub args: PgArguments,
    pub types: Vec<Type>,
    pub lock: ObjectLock,
}

impl QueryBuilder {
    pub fn new(table: &'static str) -> Self {
        Self {
            table,
            selects: "*".to_owned(),
            wheres: Vec::new(),
            limit: None,
            offset: None,
            order: None,
            args: PgArguments::default(),
            types: Vec::new(),
            lock: ObjectLock::LockShare,
        }
    }

    pub fn add_where(&mut self, clause: impl Into<String>) -> &mut Self {
        self.wheres.push(clause.into());
        self
    }

    pub fn bind(&mut self, value: impl ToSql + Send + Sync + 'static, ty: Type) -> String {
        // TODO: optimize
        self.args.push(Box::new(value));
        self.types.push(ty);
        format!("${}", self.args.len())
    }

    pub fn bind_string(&mut self, value: String) -> String {
        self.bind(value, Type::TEXT)
    }

    fn build_stmt(&self) -> String {
        let mut res = "select ".to_owned();
        assert!(!self.selects.is_empty());

        res.push_str(&self.selects);
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
        match self.lock {
            ObjectLock::LockShare => res += " for share",
            ObjectLock::LockExclusive => res += " for update",
            ObjectLock::LockNone => {}
        }

        res
    }

    pub fn build(self) -> (String, PgArguments, Vec<Type>) {
        (self.build_stmt(), self.args, self.types)
    }
}

pub fn args_to_ref(args: &PgArguments) -> Vec<&(dyn ToSql + Sync)> {
    args.iter()
        .map(|it| it.as_ref() as &(dyn ToSql + Sync))
        .collect()
}
