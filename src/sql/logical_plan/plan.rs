use crate::tuple::schema::Schema;

use super::expr::{BooleanBinaryExpr, LogicalExpr};

pub enum LogicalPlan {
    Projection(Box<Projection>),
    Scan(Scan),
    Filter(Box<Filter>),
    CreateTable(Box<CreateTable>),
    Explain(Box<Explain>),
    Insert(Box<Insert>),
    Values(Values),
    DropTables(DropTables),
    Truncate(Truncate),
    Empty,
}

impl Default for LogicalPlan {
    fn default() -> Self {
        Self::Empty
    }
}

impl LogicalPlan {
    #[allow(unused)]
    pub fn print(&self) -> String {
        self.print_indent(1)
    }

    fn print_indent(&self, indent: usize) -> String {
        match self {
            LogicalPlan::Scan(s) => s.print(indent),
            LogicalPlan::Filter(f) => f.print(indent),
            LogicalPlan::Projection(p) => p.print(indent),
            LogicalPlan::CreateTable(c) => c.print(indent),
            LogicalPlan::Explain(e) => e.print(indent),
            LogicalPlan::Insert(i) => i.print(indent),
            LogicalPlan::Values(v) => v.print(indent),
            LogicalPlan::DropTables(d) => d.print(indent),
            LogicalPlan::Truncate(t) => t.print(indent),
            LogicalPlan::Empty => format!("{} Empty", "-".repeat(indent * 2)),
        }
    }

    pub fn schema(&self) -> Schema {
        match self {
            LogicalPlan::Scan(s) => s.schema(),
            LogicalPlan::Filter(f) => f.schema(),
            LogicalPlan::Projection(p) => p.schema(),
            LogicalPlan::CreateTable(c) => c.schema(),
            LogicalPlan::Explain(e) => e.schema(),
            LogicalPlan::Insert(i) => i.schema(),
            LogicalPlan::Values(v) => v.schema(),
            LogicalPlan::DropTables(d) => d.schema(),
            LogicalPlan::Truncate(t) => t.schema(),
            LogicalPlan::Empty => Schema::new(vec![]),
        }
    }
}

pub struct Truncate {
    pub table_name: String,
}

impl Truncate {
    pub fn new(table_name: String) -> Self {
        Self { table_name }
    }

    pub fn schema(&self) -> Schema {
        Schema::default()
    }

    pub fn print(&self, indent: usize) -> String {
        format!("{} Truncate: {}", "-".repeat(indent * 2), self.table_name)
    }
}

pub struct DropTables {
    pub table_names: Vec<String>,
    pub if_exists: bool,
}

impl DropTables {
    pub fn new(table_names: Vec<String>, if_exists: bool) -> Self {
        Self {
            table_names,
            if_exists,
        }
    }

    fn name(&self) -> String {
        "DropTable".to_string()
    }

    fn schema(&self) -> Schema {
        Schema::new(vec![])
    }

    fn print(&self, indent: usize) -> String {
        format!(
            "{} {}: [{}]",
            "-".repeat(indent * 2),
            self.name(),
            self.table_names
                .iter()
                .map(|v| format!("#{}", v))
                .collect::<Vec<_>>()
                .join(", ")
        )
    }
}

pub struct Values {
    pub rows: Vec<Vec<LogicalExpr>>,
    pub schema: Schema,
}

impl Values {
    pub fn new(rows: Vec<Vec<LogicalExpr>>, schema: Schema) -> Self {
        Self { rows, schema }
    }

    fn name(&self) -> String {
        "Values".to_string()
    }

    fn schema(&self) -> Schema {
        self.schema.clone()
    }

    fn print(&self, indent: usize) -> String {
        format!(
            "{} {}: [{}]",
            "-".repeat(indent * 2),
            self.name(),
            self.rows
                .iter()
                .map(|row| format!(
                    "({})",
                    row.iter().map(|v| v.print()).collect::<Vec<_>>().join(", ")
                ))
                .collect::<Vec<_>>()
                .join(",")
        )
    }
}

pub struct Insert {
    pub input: LogicalPlan,
    pub table_name: String,
    pub table_schema: Schema,
    pub schema: Schema, // RETURNING statement
}

impl Insert {
    pub fn new(
        input: LogicalPlan,
        table_name: String,
        table_schema: Schema,
        schema: Schema,
    ) -> Self {
        Self {
            input,
            table_name,
            table_schema,
            schema,
        }
    }

    fn name(&self) -> String {
        "Insert".to_string()
    }

    fn schema(&self) -> Schema {
        self.schema.clone()
    }

    fn print(&self, indent: usize) -> String {
        format!(
            "{} {}: {}\n{}",
            "-".repeat(indent * 2),
            self.name(),
            self.table_name,
            self.input.print_indent(indent + 1)
        )
    }
}

pub struct Explain {
    pub input: LogicalPlan,
    pub analyze: bool,
}

impl Explain {
    pub fn new(input: LogicalPlan, analyze: bool) -> Self {
        Self { input, analyze }
    }

    fn name(&self) -> String {
        "Explain".to_string()
    }

    fn schema(&self) -> Schema {
        self.input.schema()
    }

    fn print(&self, indent: usize) -> String {
        format!(
            "{} {}:\n{}",
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
            "{} {}: {} [skip if exists: {}]\n{}",
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
            "{} {}: {} [{}]\n",
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
            "{} {}: {}\n{}",
            "-".repeat(indent * 2),
            self.name(),
            self.expr.print(),
            self.input.print_indent(indent + 1)
        )
    }
}

pub struct Projection {
    pub input: LogicalPlan,
    pub projections: Vec<LogicalExpr>,
}

impl Projection {
    pub fn new(input: LogicalPlan, projections: Vec<LogicalExpr>) -> Self {
        Self { input, projections }
    }

    pub fn name(&self) -> String {
        "Projection".to_string()
    }

    pub fn schema(&self) -> Schema {
        let fields = self
            .projections
            .iter()
            .map(|p| p.to_field(&self.input.schema()))
            .collect();

        Schema::new(fields)
    }

    fn print(&self, indent: usize) -> String {
        format!(
            "{} {}: [{}]\n{}",
            "-".repeat(indent * 2),
            self.name(),
            self.projections
                .iter()
                .map(|s| {
                    match s {
                        LogicalExpr::Column(c) => format!("#{}", c),
                        LogicalExpr::Literal(l) => format!("{}", l),
                        LogicalExpr::BinaryExpr(b) => format!("({})", b.print()),
                    }
                })
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
            schema: Schema::new(vec![Field::new("a", Types::UInt, false)]),
        });

        let string = scan.print();

        assert_eq!(string, "-- Scan: test [#a]\n");

        let filter = LogicalPlan::Filter(Box::new(Filter {
            expr: BooleanBinaryExpr::new(
                LogicalExpr::Column("a".to_string()),
                BinaryOperator::Gt,
                LogicalExpr::Literal(ValueFactory::from_string(&Types::UInt, "10")),
            ),
            input: scan,
        }));

        assert_eq!(filter.print(), "-- Filter: #a > 10\n---- Scan: test [#a]\n");

        Ok(())
    }
}
