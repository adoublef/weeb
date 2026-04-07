use crate::weeb::Handler;
use axum::{
    Router,
    body::Body,
    extract::{Query, State},
    response::{IntoResponse, Response},
    routing::get,
};
use http::{
    StatusCode,
    header::{CONTENT_DISPOSITION, CONTENT_TYPE},
};
use mime::APPLICATION_OCTET_STREAM;
use serde::Deserialize;
use url::Url;

pub fn app() -> Router {
    Router::new()
        .route("/", get(handle_zip))
        .with_state(AppState {
            handler: Handler::default(),
        })
}

#[derive(Deserialize)]
struct ZipQuery {
    series_url: Url,
    deflate: Option<bool>,
}

async fn handle_zip(
    State(AppState { handler }): State<AppState>,
    Query(query): Query<ZipQuery>,
) -> anyhow::Result<Response, AppError> {
    let stream = handler.series_stream(query.series_url, query.deflate.unwrap_or_default());
    let response = Response::builder()
        .header(CONTENT_TYPE, APPLICATION_OCTET_STREAM.essence_str())
        .header(CONTENT_DISPOSITION, "attachment; filename=\"weeb.zip\"")
        .status(StatusCode::OK)
        .body(Body::from_stream(stream))?;
    Ok(response)
}

#[derive(Debug, Clone)]
struct AppState {
    handler: Handler,
}

#[derive(Debug)]
struct AppError(anyhow::Error);

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Something went wrong: {}", self.0),
        )
            .into_response()
    }
}

impl<E> From<E> for AppError
where
    E: Into<anyhow::Error>,
{
    fn from(err: E) -> Self {
        Self(err.into())
    }
}

#[derive(Debug, Clone, Default)]
pub struct HttpClient(pub reqwest::Client);

impl HttpClient {
    //
}
