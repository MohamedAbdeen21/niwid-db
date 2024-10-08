use crate::tuple::schema::Schema;

use super::expr::BooleanBinaryExpr;

#[allow(dead_code)]
pub fn build_initial_plan() -> LogicalPlan {
    todo!()
}

pub enum LogicalPlan {
    Projection(Box<Projection>),
    Scan(Scan),
    #[allow(unused)]
    Filter(Box<Filter>),
}

impl LogicalPlan {
    pub fn print(&self) -> String {
        self.print_indent(0)
    }

    fn print_indent(&self, indent: usize) -> String {
        match self {
            LogicalPlan::Scan(s) => s.print(indent),
            LogicalPlan::Filter(f) => f.print(indent),
            LogicalPlan::Projection(p) => p.print(indent),
        }
    }

    pub fn schema(&self) -> Schema {
        match self {
            LogicalPlan::Scan(s) => s.schema(),
            LogicalPlan::Filter(f) => f.schema(),
            LogicalPlan::Projection(p) => p.schema(),
        }
    }
}

pub struct Scan {
    table_name: String,
    schema: Schema,
}

impl Scan {
    pub fn new(table_name: String, schema: Schema) -> Self {
        Self { table_name, schema }
    }

    fn name(&self) -> String {
        "Scan".to_string()
    }

    fn schema(&self) -> Schema {
        self.schema.clone()
    }

    fn print(&self, indent: usize) -> String {
        let mut s = String::new();
        for _ in 0..indent * 2 {
            s.push(' ');
        }
        s.push_str(&self.name());
        s.push_str(": ");
        s.push_str(&self.table_name);
        s.push_str(" [");
        s.push_str(
            &self
                .schema
                .fields
                .iter()
                .map(|f| format!("#{}", f.name.clone()))
                .collect::<Vec<_>>()
                .join(","),
        );
        s.push(']');
        s.push('\n');
        s
    }
}

pub struct Filter {
    input: LogicalPlan,
    expr: BooleanBinaryExpr,
}

impl Filter {
    #[allow(unused)]
    pub fn new(input: LogicalPlan, expr: BooleanBinaryExpr) -> Self {
        Self { input, expr }
    }

    fn name(&self) -> String {
        "Filter".to_string()
    }

    fn schema(&self) -> Schema {
        match &self.input {
            LogicalPlan::Scan(s) => s.schema(),
            LogicalPlan::Filter(f) => f.schema(),
            _ => Schema::new(vec![]),
        }
    }

    fn print(&self, indent: usize) -> String {
        let mut s = String::new();
        for _ in 0..indent {
            s.push(' ');
        }
        s.push_str(&self.name());
        s.push_str(": ");
        s.push_str(&self.expr.print());
        s.push('\n');
        s.push_str(&self.input.print_indent(indent + 1));
        s
    }
}

#[allow(dead_code)]
pub struct Projection {
    input: LogicalPlan,
    projections: Vec<String>,
}

#[allow(dead_code)]
impl Projection {
    pub fn new(input: LogicalPlan, projections: Vec<String>) -> Self {
        Self { input, projections }
    }

    pub fn name(&self) -> String {
        "Projection".to_string()
    }

    pub fn schema(&self) -> Schema {
        let schema = self.input.schema();
        if self.projections.is_empty() {
            schema
        } else {
            schema.subset(&self.projections)
        }
    }

    fn print(&self, indent: usize) -> String {
        let mut s = String::new();
        for _ in 0..indent * 2 {
            s.push(' ');
        }
        s.push_str(&self.name());
        s.push_str(": [");
        s.push_str(
            &self
                .projections
                .iter()
                .map(|s| format!("#{}", s))
                .collect::<Vec<_>>()
                .join(","),
        );
        s.push(']');
        s.push('\n');
        s.push_str(&self.input.print_indent(indent + 1));
        s
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        sql::logical_plan::expr::LogicalExpr,
        tuple::schema::Field,
        types::{Types, ValueFactory},
    };

    use super::*;
    use anyhow::Result;
    use sqlparser::ast::BinaryOperator;

    #[test]
    fn test_print() -> Result<()> {
        let scan = LogicalPlan::Scan(Scan {
            table_name: "test".to_string(),
            schema: Schema::new(vec![Field::new("a", Types::I64, false)]),
        });

        let string = scan.print();

        assert_eq!(string, "Scan: test [#a]\n");

        let filter = LogicalPlan::Filter(Box::new(Filter {
            expr: BooleanBinaryExpr::new(
                LogicalExpr::Column("a".to_string()),
                BinaryOperator::Gt,
                LogicalExpr::Literal(ValueFactory::from_string(&Types::I64, "10")),
            ),
            input: scan,
        }));

        assert_eq!(filter.print(), "Filter: #a > 10\n  Scan: test [#a]\n");

        Ok(())
    }
}
