use axum::{
    Router,
    extract::{Path, State},
    response::Html,
    routing::{get, get_service},
};
use divan::{AllocProfiler, Bencher};
use futures::TryStreamExt as _;
use reqwest::Client;
use tokio::{net::TcpListener, task::JoinSet};
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
            let (client, api_url) = api_serve(&mut set, token.clone(), arg.0, arg.1).await?;
            let (client, url) = serve(&mut set, token.clone(), client).await?;
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

async fn api_serve(
    set: &mut JoinSet<anyhow::Result<()>>,
    token: CancellationToken,
    num_chapters: usize,
    num_images: usize,
) -> anyhow::Result<(Client, Url)> {
    let listener = TcpListener::bind("0.0.0.0:0").await?;
    let addr = listener.local_addr()?;

    let client = Client::builder().build()?; // modify the client
    let url = Url::parse(&format!("http://{addr}"))?;

    let app = Router::new()
        .route(
            "/series/{series}/full-chapter-list",
            get(async move |State(url): State<_>| Html(template::series(num_chapters, url))),
        )
        .route(
            "/chapters/{chapters}/images",
            get(
                async move |State(url): State<_>, Path(chapter_id): Path<_>| {
                    Html(template::chapter(num_images, url, chapter_id))
                },
            ),
        )
        .route(
            "/images/{image}",
            get_service(ServeFile::new("assets/image.jpg")),
        )
        .with_state(url.clone());

    set.spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async move { token.cancelled().await })
            .await?;
        Ok(())
    });

    Ok((client, url))
}

async fn serve(
    set: &mut JoinSet<anyhow::Result<()>>,
    token: CancellationToken,
    _api_client: Client,
) -> anyhow::Result<(Client, Url)> {
    let listener = TcpListener::bind("0.0.0.0:0").await?;
    let addr = listener.local_addr()?;

    let client = Client::new();
    let url = Url::parse(&format!("http://{addr}"))?;

    set.spawn(async move {
        axum::serve(listener, app())
            .with_graceful_shutdown(async move { token.cancelled().await })
            .await?;
        Ok(())
    });

    Ok((client, url))
}
