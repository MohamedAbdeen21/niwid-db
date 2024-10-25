use std::collections::HashSet;

use crate::types::Types;
use anyhow::{anyhow, Result};
use bincode::{deserialize, serialize};
use serde::{Deserialize, Serialize};
use sqlparser::ast::ColumnDef;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Field {
    pub name: String,
    pub ty: Types,
    pub nullable: bool,
}

impl Default for Field {
    fn default() -> Self {
        Self {
            name: String::new(),
            ty: Types::Int,
            nullable: false,
        }
    }
}

impl Field {
    pub fn new(name: &str, ty: Types, nullable: bool) -> Self {
        Self {
            name: name.to_string(),
            ty,
            nullable,
        }
    }
}

#[derive(Default, Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Schema {
    pub fields: Vec<Field>,
    is_qualified: bool,
}

impl Schema {
    pub fn new(fields: Vec<Field>) -> Self {
        let is_qualified = fields.iter().any(|f| f.name.contains('.'));
        Self {
            fields,
            is_qualified,
        }
    }

    pub fn is_qualified(&self) -> bool {
        self.is_qualified
    }

    #[allow(unused)]
    pub fn to_sql(&self) -> String {
        let mut sql = String::new();
        for (i, field) in self.fields.iter().enumerate() {
            if i != 0 {
                sql.push(',');
            }
            sql.push_str(&field.name);
            sql.push(' ');
            sql.push_str(&field.ty.to_sql());
            if !field.nullable {
                sql.push_str(" NOT NULL");
            }
        }
        sql
    }

    pub fn join(&self, schema: Schema) -> Result<Self> {
        let mut fields = self.fields.clone();
        let left_set: HashSet<&str> =
            HashSet::from_iter(self.fields.iter().map(|f| f.name.as_str()));
        let right_set = HashSet::from_iter(schema.fields.iter().map(|f| f.name.as_str()));
        if left_set.intersection(&right_set).count() > 0 {
            return Err(anyhow!("Ambiguous column name"));
        } else {
            fields.extend(schema.fields);
        }
        Ok(Schema::new(fields))
    }

    pub fn from_sql(cols: Vec<ColumnDef>) -> Self {
        let fields = cols
            .iter()
            .map(|col| {
                let ColumnDef {
                    name,
                    data_type,
                    options,
                    ..
                } = col;

                // TODO: actually check the vec of structs for the value
                let nullable = options.is_empty();

                Field::new(
                    &name.value,
                    Types::from_sql(&data_type.to_string()),
                    nullable,
                )
            })
            .collect();

        Schema::new(fields)
    }

    pub fn add_qualifier(&self, name: &str) -> Self {
        let mut fields = self.fields.clone();
        fields
            .iter_mut()
            .for_each(|f| f.name = format!("{}.{}", name, f.name));
        Schema::new(fields)
    }
}

impl Schema {
    pub fn to_bytes(&self) -> Box<[u8]> {
        let x = serialize(self).unwrap();
        x.into_boxed_slice()
    }

    pub fn from_bytes(bytes: &[u8]) -> Self {
        deserialize(bytes).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;
    use sqlparser::ast::{CreateTable, Statement};
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    #[test]
    fn test_to_sql() -> Result<()> {
        let schema = Schema::new(vec![
            Field::new("a", Types::Int, false),
            Field::new("b", Types::Str, true),
            Field::new("c", Types::UInt, false),
        ]);

        let sql = format!(
            "CREATE TABLE users (
                {}
            )",
            schema.to_sql()
        );

        let statment = Parser::new(&GenericDialect)
            .try_with_sql(&sql)?
            .parse_statement()?;

        match statment {
            Statement::CreateTable(CreateTable { columns, .. }) => {
                assert_eq!(Schema::from_sql(columns), schema);
            }
            _ => panic!(),
        }

        Ok(())
    }

    #[test]
    fn test_from_sql() -> Result<()> {
        let sql = "CREATE TABLE users (
            a int NOT NULL,
            b text,
            c uint
        )";

        let statment = Parser::new(&GenericDialect)
            .try_with_sql(sql)?
            .parse_statement()?;

        match statment {
            Statement::CreateTable(CreateTable { columns, .. }) => {
                assert_eq!(
                    Schema::from_sql(columns),
                    Schema::new(vec![
                        Field::new("a", Types::Int, false),
                        Field::new("b", Types::Str, true),
                        Field::new("c", Types::UInt, true),
                    ])
                );
            }
            _ => unreachable!(),
        }

        Ok(())
    }
}
