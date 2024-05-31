use std::fmt::{Debug, Write};
use tokio_postgres::types::{ToSql, Type};

pub type PgArguments = Vec<Box<dyn ToSql + Send + Sync>>;

pub fn pg_pattern_escape(s: &str) -> String {
    s.replace('%', "\\%").replace('_', "\\_")
}

pub struct QueryBuilder {
    table: &'static str,
    selects: String,
    wheres: Vec<String>,
    pub order: Option<String>,
    limit: Option<u32>,
    offset: Option<u32>,
    args: PgArguments,
    types: Vec<Type>,
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
        }
    }

    pub fn select(&mut self, selects: impl Into<String>) -> &mut Self {
        self.selects = selects.into();
        self
    }

    pub fn add_where(&mut self, clause: impl Into<String>) -> &mut Self {
        self.wheres.push(clause.into());
        self
    }

    pub fn bind(&mut self, value: impl ToSql + Send + Sync + Debug + 'static, ty: Type) -> String {
        // TODO: optimize
        self.args.push(Box::new(value));
        self.types.push(ty);
        format!("${}", self.args.len())
    }

    pub fn bind_string(&mut self, value: String) -> String {
        self.bind(value, Type::TEXT)
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
