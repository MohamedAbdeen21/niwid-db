pub mod result_set;

use crate::catalog::Catalog;
use crate::sql::logical_plan::plan::{CreateTable, Filter, LogicalPlan, Scan};
use crate::sql::logical_plan::plan::{Explain, Projection};
use anyhow::Result;
use result_set::ResultSet;

pub trait Executable {
    fn execute(&self) -> Result<ResultSet>;
}

impl LogicalPlan {
    pub fn execute(&self) -> Result<ResultSet> {
        match self {
            LogicalPlan::Projection(plan) => plan.execute(),
            LogicalPlan::Scan(scan) => scan.execute(),
            LogicalPlan::Filter(filter) => filter.execute(),
            LogicalPlan::CreateTable(create) => create.execute(),
            LogicalPlan::Explain(explain) => explain.execute(),
            LogicalPlan::Empty => Ok(ResultSet::default()),
        }
    }
}

impl Executable for Explain {
    fn execute(&self) -> Result<ResultSet> {
        self.input.execute()?.show();
        Ok(ResultSet::default())
    }
}

impl Executable for CreateTable {
    fn execute(&self) -> Result<ResultSet> {
        let catalog = Catalog::get();
        catalog
            .lock()
            .add_table(&self.table_name, &self.schema, self.if_not_exists)
            .unwrap();
        Ok(ResultSet::default())
    }
}

impl Executable for Projection {
    fn execute(&self) -> Result<ResultSet> {
        let input = self.input.execute()?;
        let cols = input
            .cols
            .iter()
            .cloned()
            .enumerate()
            .filter(|(_, col)| self.projections.contains(&col.name))
            .collect::<Vec<_>>();

        let indexes = cols.iter().map(|(i, _)| *i).collect::<Vec<_>>();

        let output = input
            .data
            .iter()
            .map(|row| indexes.iter().map(|i| row[*i].clone()).collect::<Vec<_>>())
            .collect::<Vec<_>>();

        Ok(ResultSet::new(
            cols.into_iter().map(|(_, col)| col).collect::<Vec<_>>(),
            output,
        ))
    }
}

impl Executable for Scan {
    fn execute(&self) -> Result<ResultSet> {
        let arc_catalog = Catalog::get();
        let mut catalog = arc_catalog.lock();
        let table = catalog.get_table(&self.table_name).unwrap();

        let schema = table.get_schema();

        let mut values = vec![];
        table.scan(|(_, (_, tuple))| {
            values.push(tuple.get_values(&schema)?);
            Ok(())
        })?;

        Ok(ResultSet::new(schema.fields.clone(), values))
    }
}

impl Executable for Filter {
    fn execute(&self) -> Result<ResultSet> {
        let input = self.input.execute()?;

        let mut output = vec![];
        for row in input.data {
            // TODO: evaluate the expression
            output.push(row);
        }

        Ok(ResultSet::new(input.cols, output))
    }
}
