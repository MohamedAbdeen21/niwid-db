use askama::Template;
use idk::execution::result_set::ResultSet;

#[derive(Template, Default)]
#[template(path = "query_result.html")]
struct QueryResultTemplate {
    rows: Vec<Vec<String>>,
    headers: Vec<(String, String)>,
}

#[derive(Template)]
#[template(path = "additional_info.html")]
struct AdditionalInfo {
    message: Option<String>,
    error: Option<String>,
}

pub fn format_result(result: ResultSet) -> String {
    // TODO: Each execution plan should fill the info field accordingly
    // this is used as a fallback message
    if result.is_empty() && result.get_info().is_empty() {
        return QueryResultTemplate::default().render().unwrap() + format_message("OK").as_str();
    }

    let data = QueryResultTemplate {
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
    }
    .render()
    .unwrap();

    let info = format_message(result.get_info());

    data + info.as_str()
}

pub fn format_error(error: String) -> String {
    AdditionalInfo {
        message: None,
        error: Some(error),
    }
    .render()
    .unwrap()
}

pub fn format_message(message: &str) -> String {
    AdditionalInfo {
        message: Some(message.to_string()),
        error: None,
    }
    .render()
    .unwrap()
}
