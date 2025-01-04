use std::collections::HashSet;

use crate::{errors::Error, types::Types};
use anyhow::{bail, Result};
use bincode::{deserialize, serialize};
use serde::{Deserialize, Serialize};
use sqlparser::ast::{ColumnDef, ColumnOption, ColumnOptionDef};

use super::constraints::Constraints;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Field {
    pub name: String,
    pub ty: Types,
    pub constraints: Constraints,
}

impl Default for Field {
    fn default() -> Self {
        Self {
            name: String::new(),
            ty: Types::Int,
            constraints: Constraints::default(),
        }
    }
}

impl Field {
    pub fn new(name: &str, ty: Types, constraints: Constraints) -> Self {
        Self {
            name: name.to_string(),
            ty,
            constraints,
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

    #[cfg(test)]
    pub fn to_sql(&self) -> String {
        let mut sql = String::new();
        for (i, field) in self.fields.iter().enumerate() {
            if i != 0 {
                sql.push(',');
            }
            sql.push_str(&field.name);
            sql.push(' ');
            sql.push_str(&field.ty.to_sql());
            if !field.constraints.nullable {
                sql.push_str(" NOT NULL");
            }
            if field.constraints.unique {
                sql.push_str(" UNIQUE");
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
            bail!(Error::Expected(
                "qualified fields".into(),
                "unqualified field".into()
            ));
        } else {
            fields.extend(schema.fields);
        }

        Ok(Schema::new(fields))
    }

    pub fn from_sql(cols: Vec<ColumnDef>) -> Result<Self> {
        let mut has_unqiue = false;
        let fields = cols
            .iter()
            .map(|col| {
                let ColumnDef {
                    name,
                    data_type,
                    options,
                    ..
                } = col;

                let unique = options.iter().any(|opt| {
                    matches!(
                        opt,
                        ColumnOptionDef {
                            option: ColumnOption::Unique { .. },
                            ..
                        }
                    )
                });

                let not_null = options.iter().any(|opt| {
                    matches!(
                        opt,
                        ColumnOptionDef {
                            option: ColumnOption::NotNull { .. },
                            ..
                        } | ColumnOptionDef {
                            option: ColumnOption::Unique {
                                is_primary: true,
                                ..
                            },
                            ..
                        }
                    )
                });

                if unique {
                    if has_unqiue {
                        bail!(Error::Unsupported(
                            "Only one unique field is allowed".into()
                        ))
                    } else {
                        has_unqiue = true;
                    }
                }

                let type_ = Types::from_sql(&data_type.to_string())?;
                if unique && !matches!(type_, Types::UInt | Types::Int | Types::Float) {
                    bail!(Error::Unsupported(
                        "Unique field must be of type uint, int, or float".into()
                    ));
                };

                if unique && !not_null {
                    bail!(Error::Unimplemented(
                        "Nulls are not allowed in UNIQUE columns. Add NOT NULL constraint.".into()
                    ))
                };

                Ok(Field::new(
                    &name.value,
                    type_,
                    Constraints::new(!not_null, unique),
                ))
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(Schema::new(fields))
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
    use crate::errors::Error;
    use anyhow::Result;
    use sqlparser::ast::{CreateTable, Statement};
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    #[test]
    fn test_to_sql() -> Result<()> {
        let schema = Schema::new(vec![
            Field::new("a", Types::Int, Constraints::nullable(false)),
            Field::new("b", Types::Str, Constraints::nullable(true)),
            Field::new("c", Types::UInt, Constraints::nullable(false)),
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
                assert_eq!(Schema::from_sql(columns)?, schema);
            }
            e => bail!(Error::Expected("CreateTable".into(), e.to_string())),
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
                    Schema::from_sql(columns)?,
                    Schema::new(vec![
                        Field::new("a", Types::Int, Constraints::nullable(false)),
                        Field::new("b", Types::Str, Constraints::nullable(true)),
                        Field::new("c", Types::UInt, Constraints::nullable(true)),
                    ])
                );
            }
            _ => unreachable!(),
        }

        Ok(())
    }
}
