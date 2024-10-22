use crate::tuple::schema::Field;
use crate::types::Value;

#[derive(Default, Debug)]
pub struct ResultSet {
    pub fields: Vec<Field>,
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

        let cap = cols.first().map_or(0, Vec::len);
        Self { fields, cap, cols }
    }

    pub fn fields(&self) -> &Vec<Field> {
        &self.fields
    }

    pub fn from_col(field: Field, col: Vec<Value>) -> Self {
        let cap = col.len();
        Self {
            fields: vec![field],
            cap,
            cols: vec![col],
        }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            fields: Vec::with_capacity(capacity),
            cols: Vec::with_capacity(capacity),
            cap: capacity,
        }
    }

    pub fn size(&self) -> usize {
        self.cap
    }

    pub fn union(mut self, other: ResultSet) -> Self {
        // Ensure schema matches
        if self.fields.iter().map(|f| f.ty.clone()).collect::<Vec<_>>()
            != other
                .fields
                .iter()
                .map(|f| f.ty.clone())
                .collect::<Vec<_>>()
        {
            panic!("Schema mismatch");
        }

        for (i, col) in other.cols.into_iter().enumerate() {
            self.cols[i].extend(col);
        }

        self.cap = self.cols.first().map_or(0, Vec::len);
        self
    }

    pub fn concat(mut self, other: ResultSet) -> Self {
        self.fields.extend(other.fields);
        self.cols.extend(other.cols);
        self
    }

    pub fn cols(&self) -> &Vec<Vec<Value>> {
        &self.cols
    }

    pub fn rows(&self) -> Vec<Vec<Value>> {
        (0..self.size())
            .map(|i| {
                self.cols()
                    .iter()
                    .map(|col| col[i].clone())
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>()
    }

    pub fn show(&self) {
        let col_widths: Vec<usize> = self
            .fields
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

        for row_idx in 0..self.cap {
            for (i, col) in self.cols.iter().enumerate() {
                print!(
                    "| {:^width$} ",
                    format!("{}", col[row_idx]),
                    width = col_widths[i]
                );
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
