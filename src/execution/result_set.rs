use crate::{tuple::schema::Field, types::Value};

#[derive(Default, Debug)]
pub struct ResultSet {
    pub fields: Vec<Field>,
    pub data: Vec<Vec<Value>>,
    cap: usize,
}

impl ResultSet {
    pub fn new(cols: Vec<Field>, data: Vec<Vec<Value>>) -> Self {
        Self {
            fields: cols,
            cap: data.len(),
            data,
        }
    }

    pub fn from_col(field: Field, col: Vec<Value>) -> Self {
        let rows: Vec<_> = col.into_iter().map(|value| vec![value]).collect();

        Self {
            fields: vec![field],
            cap: rows.len(),
            data: rows,
        }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            fields: Vec::with_capacity(capacity),
            data: Vec::with_capacity(capacity),
            cap: capacity,
        }
    }

    pub fn size(&self) -> usize {
        self.cap
    }

    pub fn union(mut self, other: ResultSet) -> Self {
        if self.fields.iter().map(|c| c.ty.clone()).collect::<Vec<_>>()
            != other
                .fields
                .iter()
                .map(|c| c.ty.clone())
                .collect::<Vec<_>>()
        {
            panic!("Schema mismatch");
        }

        self.data.extend(other.data);
        self
    }

    pub fn concat(mut self, other: ResultSet) -> Self {
        self.data
            .iter_mut()
            .zip(other.data)
            .for_each(|(a, b)| a.extend(b));

        let cols = self.fields.into_iter().chain(other.fields).collect();

        Self {
            fields: cols,
            cap: self.data.len(),
            data: self.data,
        }
    }

    pub fn show(&self) {
        let col_widths: Vec<usize> = self
            .fields
            .iter()
            .enumerate()
            .map(|(i, col)| {
                let header_len = format!("{} ({})", col.name, col.ty.to_sql()).len();
                let max_data_len = self
                    .data
                    .iter()
                    .map(|row| format!("{}", row[i]).len())
                    .max()
                    .unwrap_or(0);
                header_len.max(max_data_len)
            })
            .collect();

        print_row_divider(&col_widths);

        for (i, col) in self.fields.iter().enumerate() {
            print!(
                "| {:^width$} ",
                format!("{} ({})", col.name, col.ty.to_sql()),
                width = col_widths[i]
            );
        }
        println!("|");

        print_row_divider(&col_widths);

        for row in self.data.iter() {
            for (i, value) in row.iter().enumerate() {
                print!("| {:^width$} ", format!("{}", value), width = col_widths[i]);
            }
            println!("|");

            print_row_divider(&col_widths);
        }
    }
}

fn print_row_divider(col_widths: &[usize]) {
    for &width in col_widths {
        print!("+{:-<width$}", "-", width = width + 2); // +---+
    }
    println!("+");
}
