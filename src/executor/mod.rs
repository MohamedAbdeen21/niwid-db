mod result_set;

use crate::catalog::Catalog;
use crate::executor::result_set::ResultSet;
use crate::table::Table;
use crate::tuple::schema::Schema;
use crate::tuple::Tuple;
use crate::txn_manager::{ArcTransactionManager, TransactionManager, TxnId};
use crate::types::{self, Null, TypeFactory, Types};
use anyhow::{anyhow, Result};
use sqlparser::ast::*;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

pub struct Executor {
    pub catalog: Catalog,
    txn_manager: ArcTransactionManager,
    active_txn: Option<TxnId>,
    txn_tables: Vec<String>,
    catalog_changed: bool,
}

#[allow(dead_code)]
impl Executor {
    pub fn new() -> Result<Self> {
        Ok(Self {
            catalog: Catalog::new()?,
            txn_manager: TransactionManager::get(),
            active_txn: None,
            txn_tables: vec![],
            catalog_changed: false,
        })
    }

    pub fn add_table(
        &mut self,
        name: &str,
        schema: &Schema,
        ignore_if_exists: bool,
    ) -> Result<&mut Table> {
        if self.active_txn.is_some() && !self.catalog_changed {
            self.catalog.table.start_txn(self.active_txn.unwrap())?;
            self.catalog_changed = true;
        }

        let table = self.catalog.add_table(name, schema, ignore_if_exists)?;

        Ok(table)
    }

    pub fn start_txn(&mut self) -> Result<()> {
        if self.active_txn.is_some() {
            return Ok(());
        }

        let id = self.txn_manager.lock().start()?;
        self.active_txn = Some(id);

        Ok(())
    }

    pub fn commit_txn(&mut self) -> Result<()> {
        if self.active_txn.is_none() {
            return Ok(());
        }

        self.txn_manager.lock().commit(self.active_txn.unwrap())?;

        for table in self.txn_tables.iter_mut() {
            self.catalog.get_table(table).unwrap().commit_txn()?;
        }

        if self.catalog_changed {
            self.catalog.table.commit_txn()?;
            self.catalog_changed = false;
        }

        self.txn_tables.clear();
        self.active_txn = None;

        Ok(())
    }

    fn sql_expr_to_tuple(ident: Vec<Option<String>>, schema: Schema) -> Tuple {
        let values = ident
            .iter()
            .zip(schema.fields.iter().map(|f| f.ty.clone()))
            .map(|(v, ty)| match v {
                None => types::Null().into(),
                Some(v) => TypeFactory::from_string(&ty, v),
            })
            .collect();

        Tuple::new(values, &schema)
    }

    fn handle_insert(&mut self, table_name: &str, source: Option<Box<Query>>) -> Result<ResultSet> {
        let table = self.catalog.get_table(table_name).unwrap();
        let schema = table.get_schema();

        if self.active_txn.is_some() && !self.txn_tables.contains(&table_name.to_string()) {
            table.start_txn(self.active_txn.unwrap())?;
            self.txn_tables.push(table_name.to_string());
        }

        let tuple = match source {
            None => Err(anyhow!("no source provided"))?,
            Some(source) => {
                let Query { body, .. } = *source;
                let values = match *body {
                    SetExpr::Values(Values { rows, .. }) => rows,
                    _ => todo!(),
                };

                Self::sql_expr_to_tuple(
                    values
                        .iter()
                        .flatten()
                        .map(|e| match e {
                            Expr::Value(Value::Number(v, _)) => Some(v.clone()),
                            Expr::Value(Value::SingleQuotedString(v)) => Some(v.clone()),
                            Expr::Value(Value::Null) => None,
                            _ => todo!(),
                        })
                        .collect(),
                    schema,
                )
            }
        };
        table.insert(tuple)?;

        Ok(ResultSet::new(
            vec!["inserted".to_string()],
            vec![Types::I64],
            vec![],
        ))
    }

    fn handle_select(&mut self, body: SetExpr, _limit: Option<Expr>) -> Result<ResultSet> {
        println!("{:?}", body);
        let table_name = match body {
            SetExpr::Select(ref select) => match &select.from.first().unwrap().relation {
                TableFactor::Table { name, .. } => name.0.first().unwrap().value.clone(),
                _ => todo!(),
            },
            _ => unimplemented!(),
        };

        let table = self.catalog.get_table(&table_name).unwrap();
        let schema = table.get_schema();

        let columns = match body {
            SetExpr::Select(select) => select
                .projection
                .iter()
                .flat_map(|e| match e {
                    SelectItem::UnnamedExpr(Expr::Identifier(ident)) => vec![ident.value.clone()],
                    SelectItem::Wildcard(_) => {
                        schema.fields.iter().map(|f| f.name.clone()).collect()
                    }
                    _ => todo!(),
                })
                .collect::<Vec<_>>(),
            _ => unimplemented!(),
        };

        let types = schema
            .fields
            .iter()
            .map(|f| f.ty.clone())
            .collect::<Vec<_>>();

        let mut results = vec![];

        // handle duplicate columns
        table.scan(|(_, (_, tuple))| {
            let mut values = tuple.get_values(&schema)?;
            let mut result = Vec::with_capacity(columns.len());
            columns
                .iter()
                .map(|field| schema.fields.iter().position(|f| &f.name == field).unwrap())
                .try_for_each(|field| -> Result<()> {
                    let v = match &types[field] {
                        Types::Str if !values[field].is_null() => {
                            Box::new(table.fetch_string(&values[field].to_bytes()))
                        }
                        t if !values[field].is_null() => {
                            // a small trick to clone the underlying value
                            // dyn traits can't extend clone or copy
                            let bytes = values[field].to_bytes();
                            std::mem::replace(
                                &mut values[field],
                                TypeFactory::from_bytes(t, &bytes),
                            )
                        }
                        _ if values[field].is_null() => Box::new(Null()),
                        _ => todo!(),
                    };

                    result.push(v);

                    Ok(())
                })?;

            results.push(result);

            Ok(())
        })?;

        let mut output_types = vec![];

        for field in columns.iter() {
            let field = schema.fields.iter().find(|f| f.name == *field).unwrap();
            output_types.push(field.ty.clone());
        }

        Ok(ResultSet::new(columns, output_types, results))
    }

    pub fn execute_sql(&mut self, sql: &str) -> Result<ResultSet> {
        let statment = Parser::new(&GenericDialect)
            .try_with_sql(sql)?
            .parse_statement()?;

        match statment {
            Statement::Insert(Insert {
                table_name, source, ..
            }) => self.handle_insert(&table_name.0.first().unwrap().value, source),
            Statement::Query(query) => {
                let Query { body, limit, .. } = *query;
                self.handle_select(*body, limit)
            }
            Statement::CreateTable(_t) => todo!(),
            _ => unimplemented!(),
        }
    }
}
