mod common;
use anyhow::Context as _;
use async_zip::base::read::stream::ZipFileReader;
use axum::{
    Router,
    extract::{Path, State},
    response::Html,
    routing::{get, get_service},
};
use futures::TryStreamExt as _;
use http::{StatusCode, header::CONTENT_TYPE};
use mime::APPLICATION_OCTET_STREAM;
use tokio::{io::BufReader, task::JoinSet};
use tokio_util::{compat::FuturesAsyncReadCompatExt, io::StreamReader, sync::CancellationToken};
use tower_http::services::ServeFile;
use url::Url;
use weeb::{html::template, net::http::app};

#[tokio::test]
async fn handle_zip_ok() -> anyhow::Result<()> {
    let mut set = JoinSet::new();
    let token = CancellationToken::new();

    let num_chapters = 1 << 3;
    let num_images = 1 << 3;

    let (_client, api_url) = common::listen_and_serve_with_addr(
        &mut set,
        token.clone(),
        test_app(num_chapters, num_images),
    )
    .await?;
    let (client, mut url) = common::listen_and_serve(&mut set, token.clone(), app()).await?;

    url.query_pairs_mut()
        .append_pair("series_url", &format!("{api_url}series/1"))
        .append_pair("deflate", "true");

    let response = client.get(url).send().await?;
    assert_eq!(response.status(), StatusCode::OK);
    let headers = response.headers();
    let content_type = headers
        .get(CONTENT_TYPE)
        .context("Missing content-type header")?;
    assert_eq!(content_type, APPLICATION_OCTET_STREAM.as_ref());

    // write to a temp file and then parse it
    let reader = StreamReader::new(response.bytes_stream().map_err(std::io::Error::other));
    // https://github.com/Majored/rs-async-zip/releases/tag/v0.0.17
    let mut series_zip = ZipFileReader::with_tokio(reader);

    let mut num_files = 0;
    while let Some(mut entry) = series_zip.next_with_entry().await? {
        let is_dir = entry.reader().entry().dir()?;
        assert_eq!(is_dir, false);

        let reader = entry.reader_mut().compat();
        let buf_reader = BufReader::with_capacity(4 << 10, reader); // ~8 KB
        let mut chapter_zip = ZipFileReader::with_tokio(buf_reader);
        while let Some(entry) = chapter_zip.next_with_entry().await? {
            let is_dir = entry.reader().entry().dir()?;
            assert_eq!(is_dir, false);

            let size = entry.reader().entry().uncompressed_size();
            assert_eq!(size, 86387);

            chapter_zip = entry.skip().await?;
            num_files += 1;
        }

        // Close current file prior to proceeding, as per:
        // https://docs.rs/async_zip/0.0.16/async_zip/base/read/stream/
        series_zip = entry.skip().await?;
    }
    assert_eq!(num_files, num_chapters * num_images);

    token.cancel();
    for res in set.join_all().await {
        res?
    }
    assert!(token.is_cancelled());
    Ok(())
}

fn test_app(num_chapters: usize, num_images: usize) -> impl FnOnce(&Url) -> Router {
    move |url: &Url| {
        Router::new()
            .route(
                "/series/{series}/full-chapter-list",
                get(async move |State(url): State<Url>| Html(template::series(num_chapters, url))),
            )
            .route(
                "/chapters/{chapters}/images",
                get(
                    async move |State(url): State<Url>, Path(chapter_id): Path<_>| {
                        Html(template::chapter(num_images, url, chapter_id))
                    },
                ),
            )
            .route(
                "/images/{image}",
                get_service(ServeFile::new("assets/image.jpg")),
            )
            .with_state::<_>(url.clone())
    }
}
