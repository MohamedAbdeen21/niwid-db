use crate::context::tests::test_context;
use crate::context::Context;
use crate::errors::Error;
use crate::types;
use anyhow::Result;
use sqllogictest::{DBOutput, DB};

impl DB for Context {
    type Error = Error;

    type ColumnType = types::Types;

    fn run(&mut self, sql: &str) -> Result<DBOutput<Self::ColumnType>, Self::Error> {
        match self.execute_sql(sql) {
            Ok(result) => {
                let rows = result
                    .rows()
                    .iter()
                    .map(|r| r.iter().map(|v| v.to_string()).collect())
                    .collect();
                let types = result.schema.fields.into_iter().map(|f| f.ty).collect();
                Ok(sqllogictest::DBOutput::Rows { types, rows })
            }
            Err(e) => Err(Error::Internal(e.to_string())),
        }
    }
}

#[tokio::test]
async fn sqllogictest() -> Result<()> {
    let test_files = vec![
        "./src/sqllogictest/slt_files/basic.slt",
        "./src/sqllogictest/slt_files/select.slt",
    ];

    let mut handles = Vec::new();

    for file in test_files {
        let handle = tokio::spawn(async move {
            let mut tester = sqllogictest::Runner::new(|| async {
                let db = test_context();
                Ok(db)
            });

            tester.run_file_async(file).await
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.await??;
    }

    Ok(())
}
