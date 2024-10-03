mod result_set;

use crate::catalog::Catalog;
use crate::executor::result_set::ResultSet;
use crate::table::Table;
use crate::tuple::schema::Schema;
use crate::tuple::Tuple;
use crate::txn_manager::{ArcTransactionManager, TransactionManager, TxnId};
use crate::types::{self, AsBytes, Null, TypeFactory, Types, U128};
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

    fn handle_update(
        &mut self,
        table: TableWithJoins,
        assignments: Vec<Assignment>,
        _from: Option<TableWithJoins>,
        filter: Option<Expr>,
    ) -> Result<ResultSet> {
        let table_name = match table.relation {
            TableFactor::Table { name, .. } => name.0.first().unwrap().value.clone(),
            _ => todo!(),
        };

        let table = self.catalog.get_table(&table_name).unwrap();
        let schema = table.get_schema();

        let target_tuples = match filter {
            None => todo!(),
            Some(filter) => match filter {
                Expr::BinaryOp { left, op, right } => match op {
                    BinaryOperator::Eq => {
                        let col = if let Expr::Identifier(ident) = *left {
                            ident.value.clone()
                        } else {
                            todo!()
                        };

                        let value = match *right {
                            Expr::Value(Value::Number(v, _)) => Some(v),
                            Expr::Value(Value::Null) => None,
                            Expr::Value(Value::SingleQuotedString(v)) => Some(v),
                            _ => todo!(),
                        };

                        let mut tuples = vec![];
                        table.scan(|(id, (_, tuple))| {
                            let col_index =
                                schema.fields.iter().position(|f| f.name == col).unwrap();

                            let ty = schema.fields[col_index].ty.clone();

                            // TODO: handle wrong type
                            match &value {
                                None if tuple.get_values(&schema)?[col_index].is_null() => {
                                    tuples.push((id.clone(), tuple.clone()));
                                }
                                Some(v) if ty == Types::Str => {
                                    let d = tuple
                                        .get_value_at::<U128>(col_index as u8, &schema)?
                                        .unwrap();
                                    let string = table.fetch_string(&d.to_bytes()).0.clone();
                                    if string == *v {
                                        tuples.push((id.clone(), tuple.clone()));
                                    }
                                }
                                Some(v)
                                    if tuple.get_values(&schema)?[col_index].to_bytes()
                                        == TypeFactory::from_string(&ty, &v).to_bytes() =>
                                {
                                    tuples.push((id.clone(), tuple.clone()));
                                }
                                _ => (),
                            }

                            Ok(())
                        })?;

                        tuples
                    }
                    _ => todo!(),
                },
                _ => todo!(),
            },
        };

        let assign = assignments.iter().map(|assignment| match assignment {
            Assignment { target, value } => {
                let col = match target {
                    AssignmentTarget::ColumnName(col) => col.0.first().unwrap().value.as_str(),
                    _ => todo!(),
                };

                let field = schema.fields.iter().position(|f| f.name == col).unwrap();

                let ty = schema.fields[field].ty.clone();

                let v = match value {
                    Expr::Value(Value::Number(v, _)) => TypeFactory::from_string(&ty, &v.clone()),
                    Expr::Value(Value::SingleQuotedString(v)) => TypeFactory::from_string(&ty, v),
                    _ => todo!(),
                };

                (field, v)
            }
        });

        let mut new_tuples = vec![];

        for ((id, tuple), (col_id, value)) in target_tuples.iter().zip(assign) {
            let mut tuple_values = vec![];
            for (i, field) in tuple.get_values(&schema).unwrap().iter().enumerate() {
                let x = if i != col_id {
                    if matches!(schema.fields[i].ty, Types::Str) {
                        let d = tuple
                            .get_value_at::<U128>(i as u8, &schema)
                            .unwrap()
                            .unwrap();
                        let string = table.fetch_string(&d.to_bytes()).0.clone();
                        TypeFactory::from_string(&Types::Str, &string)
                    } else {
                        TypeFactory::from_bytes(&schema.fields[i].ty, &field.to_bytes())
                    }
                } else {
                    TypeFactory::from_bytes(&schema.fields[i].ty, &value.to_bytes())
                };
                tuple_values.push(x);
            }

            new_tuples.push((id, Tuple::new(tuple_values, &schema)))
        }

        new_tuples.into_iter().for_each(|(id, new_tuple)| {
            table.update(Some(*id), new_tuple).unwrap();
        });

        Ok(ResultSet::new(vec![], vec![], vec![]))
    }

    fn handle_select(&mut self, body: SetExpr, _limit: Option<Expr>) -> Result<ResultSet> {
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
            let values = tuple.get_values(&schema)?;
            let mut result = Vec::with_capacity(columns.len());
            columns
                .iter()
                .map(|field| schema.fields.iter().position(|f| &f.name == field).unwrap())
                .try_for_each(|field| -> Result<()> {
                    let v = match &types[field] {
                        _ if values[field].is_null() => Box::new(Null()) as Box<dyn AsBytes>,
                        Types::Str => Box::new(table.fetch_string(&values[field].to_bytes())),
                        ty => {
                            // a small trick to clone the underlying value
                            // dyn traits can't extend clone or copy
                            let bytes = values[field].to_bytes();
                            TypeFactory::from_bytes(ty, &bytes)
                        }
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
            Statement::Update {
                table,
                assignments,
                from,
                selection,
                ..
            } => self.handle_update(table, assignments, from, selection),
            Statement::CreateTable(_t) => todo!(),
            _ => unimplemented!(),
        }
    }
}