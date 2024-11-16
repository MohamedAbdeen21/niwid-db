use crate::context::tests::test_context;
use crate::context::Context;
use crate::errors::Error;
use crate::types;
use anyhow::Result;
use sqllogictest::DB;

impl DB for Context {
    type Error = Error;

    type ColumnType = types::Types;

    fn run(&mut self, sql: &str) -> Result<sqllogictest::DBOutput<Self::ColumnType>, Self::Error> {
        match self.execute_sql(sql) {
            Ok(result) => Ok(sqllogictest::DBOutput::Rows {
                types: result.schema.fields.iter().map(|f| f.ty.clone()).collect(),
                rows: result
                    .rows()
                    .iter()
                    .map(|r| r.iter().map(|v| v.to_string()).collect())
                    .collect(),
            }),
            Err(e) => Err(Error::Internal(e.to_string())),
        }
    }
}

#[test]
fn sqllogictests() -> Result<()> {
    let mut tester = sqllogictest::Runner::new(|| async {
        let db = test_context();
        Ok(db)
    });

    tester.run_file("./src/sqllogictest/slt_files/basic.slt")?;

    Ok(())
}
