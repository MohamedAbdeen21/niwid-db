use crate::tuple::schema::Schema;

use super::expr::BooleanBinaryExpr;

pub enum LogicalPlan {
    Projection(Box<Projection>),
    Scan(Scan),
    Filter(Box<Filter>),
    CreateTable(Box<CreateTable>),
    Explain(Box<Explain>),
    Empty,
}

impl Default for LogicalPlan {
    fn default() -> Self {
        Self::Empty
    }
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
            LogicalPlan::CreateTable(c) => c.print(indent),
            LogicalPlan::Explain(e) => e.print(indent),
            LogicalPlan::Empty => "Empty".to_string(),
        }
    }

    pub fn schema(&self) -> Schema {
        match self {
            LogicalPlan::Scan(s) => s.schema(),
            LogicalPlan::Filter(f) => f.schema(),
            LogicalPlan::Projection(p) => p.schema(),
            LogicalPlan::CreateTable(c) => c.schema(),
            LogicalPlan::Explain(e) => e.schema(),
            LogicalPlan::Empty => Schema::new(vec![]),
        }
    }
}

pub struct Explain {
    pub input: LogicalPlan,
}

impl Explain {
    pub fn new(input: LogicalPlan) -> Self {
        Self { input }
    }

    fn name(&self) -> String {
        "Explain".to_string()
    }

    fn schema(&self) -> Schema {
        self.input.schema()
    }

    fn print(&self, indent: usize) -> String {
        format!(
            "{}{}:\n{}",
            "-".repeat(indent * 2),
            self.name(),
            self.input.print_indent(indent + 1)
        )
    }
}

pub struct CreateTable {
    pub table_name: String,
    pub input: LogicalPlan,
    pub schema: Schema,
    pub if_not_exists: bool,
}

impl CreateTable {
    pub fn new(
        input: LogicalPlan,
        table_name: String,
        schema: Schema,
        if_not_exists: bool,
    ) -> Self {
        Self {
            table_name,
            input,
            schema,
            if_not_exists,
        }
    }

    fn name(&self) -> String {
        "CreateTable".to_string()
    }

    fn schema(&self) -> Schema {
        self.schema.clone()
    }

    fn print(&self, indent: usize) -> String {
        format!(
            "{}{}: {} [skip if exists: {}]\n{}",
            "-".repeat(indent * 2),
            self.name(),
            self.table_name,
            self.if_not_exists,
            self.input.print_indent(indent + 1)
        )
    }
}

pub struct Scan {
    pub table_name: String,
    pub schema: Schema,
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
        format!(
            "{}{}: {} [{}]\n",
            "-".repeat(indent * 2),
            self.name(),
            self.table_name,
            self.schema
                .fields
                .iter()
                .map(|f| format!("#{}", f.name))
                .collect::<Vec<_>>()
                .join(",")
        )
    }
}

pub struct Filter {
    pub input: LogicalPlan,
    pub expr: BooleanBinaryExpr,
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
        format!(
            "{}{}: {}\n{}",
            "-".repeat(indent * 2),
            self.name(),
            self.expr.print(),
            self.input.print_indent(indent + 1)
        )
    }
}

pub struct Projection {
    pub input: LogicalPlan,
    pub projections: Vec<String>,
}

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
        format!(
            "{}{}: [{}]\n{}",
            "-".repeat(indent * 2),
            self.name(),
            self.projections
                .iter()
                .map(|s| format!("#{}", s))
                .collect::<Vec<_>>()
                .join(","),
            self.input.print_indent(indent + 1)
        )
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

        assert_eq!(filter.print(), "Filter: #a > 10\n--Scan: test [#a]\n");

        Ok(())
    }
}
