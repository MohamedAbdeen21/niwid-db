use askama::Template;
use idk::execution::result_set::ResultSet;

#[derive(Template)]
#[template(path = "query_result.html")]
struct QueryResultTemplate {
    rows: Vec<Vec<String>>,
    headers: Vec<(String, String)>,
}

pub fn format_result(result: ResultSet) -> String {
    let template = QueryResultTemplate {
        rows: result
            .rows()
            .iter()
            .map(|row| row.iter().map(|v| v.to_string()).collect())
            .collect(),
        headers: result
            .schema
            .fields
            .iter()
            .map(|field| (field.name.clone(), field.ty.to_sql()))
            .collect(),
    };

    template.render().unwrap()
}

pub fn format_error(error: String) -> String {
    format!("<p>{}</p>", error)
}
