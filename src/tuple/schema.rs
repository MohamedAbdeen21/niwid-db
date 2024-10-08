use crate::types::Types;
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
            ty: Types::U8,
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
}

impl Schema {
    pub fn new(fields: Vec<Field>) -> Self {
        Self { fields }
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

    pub fn subset(&self, fields: &[String]) -> Self {
        let subset = fields
            .iter()
            .map(|field| {
                self.fields
                    .iter()
                    .find(|f| f.name == *field)
                    .unwrap()
                    .clone()
            })
            .collect();

        Schema::new(subset)
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
            Field::new("a", Types::I64, false),
            Field::new("b", Types::Str, true),
            Field::new("c", Types::U8, false),
        ]);

        let sql = format!(
            "CREATE TABLE users (
                {}
            )",
            schema.to_sql()
        );

        println!("{:#?}", sql);

        let statment = Parser::new(&GenericDialect)
            .try_with_sql(&sql)?
            .parse_statement()?;

        println!("{:#?}", statment);

        match statment {
            Statement::CreateTable(CreateTable { columns, .. }) => {
                assert_eq!(Schema::from_sql(columns), schema);
            }
            _ => panic!(),
        }

        Ok(())
    }
}
