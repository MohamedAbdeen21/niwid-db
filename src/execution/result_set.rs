use std::mem::take;

use crate::tuple::schema::{Field, Schema};
use crate::types::Value;

#[derive(Default, Debug, Clone, PartialEq)]
pub struct ResultSet {
    info: String,
    pub schema: Schema,
    pub cols: Vec<Vec<Value>>, // Store data as columns
    cap: usize,                // Capacity refers to the number of rows
}

impl ResultSet {
    pub fn new(fields: Vec<Field>, cols: Vec<Vec<Value>>) -> Self {
        assert!(
            cols.iter()
                .map(|col| col.len())
                .all(|len| len == cols[0].len()),
            "Column length mismatch"
        );

        let schema = Schema::new(fields);

        let cap = cols.first().map_or(0, Vec::len);
        Self {
            schema,
            cap,
            cols,
            info: String::new(),
        }
    }

    pub fn fields(&self) -> &Vec<Field> {
        &self.schema.fields
    }

    pub fn from_rows(fields: Vec<Field>, rows: Vec<Vec<Value>>) -> Self {
        let cols = (0..fields.len())
            .map(|i| rows.iter().map(|row| row[i].clone()).collect())
            .collect();

        let schema = Schema::new(fields);

        Self {
            schema,
            cols,
            cap: rows.len(),
            info: String::new(),
        }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            schema: Schema::new(Vec::with_capacity(capacity)),
            cols: Vec::with_capacity(capacity),
            cap: capacity,
            info: String::new(),
        }
    }

    pub fn take(mut self, cap: u32) -> Self {
        self.cols
            .iter_mut()
            .for_each(|col| col.truncate(cap as usize));
        Self::new(self.schema.fields, self.cols)
    }

    pub fn from_tuple(field: Vec<Field>, tuple: Vec<Value>, cap: usize) -> Self {
        let cols = (0..field.len())
            .map(|i| (0..cap).map(|_| tuple[i].clone()).collect())
            .collect();

        Self::new(field, cols)
    }

    pub fn get_info(&self) -> &str {
        &self.info
    }

    pub fn set_info(&mut self, info: String) {
        self.info = info
    }

    pub fn len(&self) -> usize {
        self.cap
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn union(mut self, other: ResultSet) -> Self {
        if self
            .fields()
            .iter()
            .zip(other.fields())
            .any(|(f1, f2)| !f1.ty.is_compatible(&f2.ty))
        {
            panic!("Schema mismatch");
        }

        for (i, col) in other.cols.into_iter().enumerate() {
            self.cols[i].extend(col);
        }

        self.cap = self.cols.first().map_or(0, Vec::len);
        self
    }

    pub fn select(mut self, indexes: Vec<usize>) -> Self {
        self.schema = Schema::new(
            indexes
                .iter()
                .map(|i| take(&mut self.schema.fields[*i]))
                .collect(),
        );
        self.cols = indexes.iter().map(|i| take(&mut self.cols[*i])).collect();
        self
    }

    pub fn concat(mut self, other: ResultSet) -> Self {
        self.schema = Schema::new(
            self.schema
                .fields
                .into_iter()
                .chain(other.schema.fields)
                .collect(),
        );
        self.cols.extend(other.cols);
        self
    }

    pub fn cols(&self) -> &Vec<Vec<Value>> {
        &self.cols
    }

    pub fn rows(&self) -> Vec<Vec<Value>> {
        (0..self.len())
            .map(|i| {
                self.cols()
                    .iter()
                    .map(|col| col[i].clone())
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>()
    }

    pub fn print(&self) -> String {
        let mut buf = String::new();

        if !self.info.is_empty() {
            buf.push_str(&self.info);
            buf.push('\n');
        }

        if self.is_empty() {
            return buf;
        }

        let col_widths: Vec<usize> = self
            .fields()
            .iter()
            .enumerate()
            .map(|(i, col)| {
                let header_len = format!("{} ({})", col.name, col.ty.to_sql()).len();
                let max_data_len = self.cols[i]
                    .iter()
                    .map(|v| format!("{}", v).len())
                    .max()
                    .unwrap_or(0);
                header_len.max(max_data_len)
            })
            .collect();

        print_row_divider(&mut buf, &col_widths);

        for (i, col) in self.fields().iter().enumerate() {
            buf.push_str(&format!(
                "| {:^width$} ",
                format!("{} ({})", col.name, col.ty.to_sql()),
                width = col_widths[i]
            ));
        }
        buf.push_str("|\n");

        print_row_divider(&mut buf, &col_widths);

        for row_idx in 0..self.cap {
            for (i, col) in self.cols.iter().enumerate() {
                buf.push_str(&format!(
                    "| {:^width$} ",
                    format!("{}", col[row_idx]),
                    width = col_widths[i],
                ));
            }
            buf.push_str("|\n");

            print_row_divider(&mut buf, &col_widths);
        }

        buf
    }
}

fn print_row_divider(buf: &mut String, col_widths: &[usize]) {
    for &width in col_widths {
        buf.push_str(&format!("+{:-<width$}", "-", width = width + 2)); // +---+
    }
    buf.push_str("+\n");
}
