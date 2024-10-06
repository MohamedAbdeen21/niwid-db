use crate::types::{Types, Value};

#[derive(Default)]
pub struct ResultSet {
    cols: Vec<String>,
    types: Vec<Types>,
    data: Vec<Vec<Value>>,
}

impl ResultSet {
    pub fn new(cols: Vec<String>, types: Vec<Types>, data: Vec<Vec<Value>>) -> Self {
        Self { cols, types, data }
    }

    pub fn show(&self) {
        let col_widths: Vec<usize> = self
            .cols
            .iter()
            .enumerate()
            .map(|(i, col)| {
                let header_len = format!("{} ({:?})", col, self.types[i]).len();
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

        for (i, (col, t)) in self.cols.iter().zip(self.types.iter()).enumerate() {
            print!(
                "| {:^width$} ",
                format!("{} ({:?})", col, t),
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
