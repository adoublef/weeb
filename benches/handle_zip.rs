mod common;
use axum::{
    Router,
    extract::{Path, State},
    response::Html,
    routing::{get, get_service},
};
use divan::{AllocProfiler, Bencher};
use futures::TryStreamExt as _;
use reqwest::Client;
use tokio::task::JoinSet;
use tokio_util::{io::StreamReader, sync::CancellationToken};
use tower_http::services::ServeFile;
use url::Url;
use weeb::{html::template, net::http::app};

#[global_allocator]
static ALLOC: AllocProfiler = AllocProfiler::system();

fn main() {
    divan::main();
}

#[derive(Debug, Clone)]
struct Arg(usize, usize);

#[divan::bench(threads = false, args = [Arg(1<<3, 1<<3), Arg(1<<2, 1<<4), Arg(1<<1, 1<<5)])]
fn handle_zip_ok(b: Bencher, arg: &Arg) {
    let rt = &tokio::runtime::Runtime::new().unwrap();
    let mut set = JoinSet::new();
    let token = CancellationToken::new();

    let token = token.clone();
    let (client, mut url, api_url) = rt
        .block_on(async {
            let (_client, api_url) =
                common::listen_and_serve_with_addr(&mut set, token.clone(), test_app(arg.0, arg.1))
                    .await?;
            let (client, url) = common::listen_and_serve(&mut set, token.clone(), app()).await?;
            anyhow::Ok((client, url, api_url))
        })
        .unwrap();

    url.query_pairs_mut()
        .append_pair("series_url", &format!("{api_url}series/1"));

    b.bench_local(|| {
        rt.block_on(async {
            let response = client.get(url.clone()).send().await?.error_for_status()?;
            let mut reader =
                StreamReader::new(response.bytes_stream().map_err(std::io::Error::other));
            let mut writer = tokio::io::sink(); // 8kb reads
            assert!(tokio::io::copy(&mut reader, &mut writer).await? > 0); // should i know this?
            anyhow::Ok(())
        })
        .unwrap();
    });

    // use the clone
    let cancelled = rt
        .block_on({
            async {
                token.cancel();
                for res in set.join_all().await {
                    res?
                }
                anyhow::Ok(token.is_cancelled())
            }
        })
        .unwrap();
    assert!(cancelled)
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
