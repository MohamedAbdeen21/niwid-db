use crate::tuple::schema::Schema;

use super::expr::BooleanBinaryExpr;

#[allow(dead_code)]
pub enum LogicalPlan {
    Projection(Projection),
    Scan(Scan),
    Filter(Filter),
}

#[allow(dead_code)]
impl LogicalPlan {
    pub fn print(&self, indent: usize) -> String {
        match self {
            LogicalPlan::Projection(p) => p.print(indent),
            LogicalPlan::Scan(s) => s.print(indent),
            LogicalPlan::Filter(f) => f.print(indent),
        }
    }
}

macro_rules! impl_logical_plan_node {
    ($struct: ident) => {
        #[allow(dead_code)]
        impl $struct {
            fn name(&self) -> String {
                String::from(stringify!($struct))
            }
            fn schema(&self) -> Schema {
                self.schema.clone()
            }

            pub fn print(&self, indent: usize) -> String {
                let mut s = String::new();
                for _ in 0..indent {
                    s.push(' ');
                }
                s.push_str(&self.name());
                s.push_str(": ");
                for field in self.schema().fields {
                    s.push_str(&format!("#{}, ", field.name));
                }
                s.push('\n');
                for child in &self.children {
                    s.push_str(&child.print(indent + 2));
                }
                s
            }
        }
    };
}

pub struct Scan {
    schema: Schema,
    children: Vec<LogicalPlan>,
}

pub struct Filter {
    schema: Schema,
    children: Vec<LogicalPlan>,
    expr: BooleanBinaryExpr,
}

pub struct Projection {
    schema: Schema,
    children: Vec<LogicalPlan>,
}

impl_logical_plan_node!(Projection);
impl_logical_plan_node!(Filter);
impl_logical_plan_node!(Scan);

#[cfg(test)]
mod tests {
    use crate::{tuple::schema::Field, types::Types};

    use super::*;
    use anyhow::Result;

    #[test]
    fn test_print() -> Result<()> {
        let scan = Scan {
            schema: Schema::new(vec![Field::new("a", Types::I64, false)]),
            children: vec![],
        };

        let string = scan.print(0);

        assert_eq!(string, "Scan: #a, \n");
        Ok(())
    }
}
