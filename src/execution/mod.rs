use crate::catalog::Catalog;
use crate::context::result_set::ResultSet;
use crate::sql::logical_plan::plan::Projection;
use crate::sql::logical_plan::plan::{Filter, LogicalPlan, Scan};
use anyhow::Result;

impl LogicalPlan {
    pub fn execute(&self) -> Result<ResultSet> {
        match self {
            LogicalPlan::Projection(plan) => plan.execute(),
            LogicalPlan::Scan(scan) => scan.execute(),
            LogicalPlan::Filter(filter) => filter.execute(),
            _ => todo!(),
        }
    }
}

impl Projection {
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

impl Scan {
    fn execute(&self) -> Result<ResultSet> {
        let mut catalog = Catalog::new()?;
        let table = catalog.get_table(&self.table_name).unwrap();

        let schema = table.get_schema();

        let mut values = vec![];
        table.scan(|(_, (_, tuple))| {
            values.push(tuple.get_values(&schema)?);
            Ok(())
        });

        Ok(ResultSet::new(schema.fields.clone(), values))
    }
}

impl Filter {
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
