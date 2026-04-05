use anyhow::Context as _;
use async_zip::base::read::seek::ZipFileReader;
use axum::{
    Router,
    extract::{Path, State},
    response::Html,
    routing::{get, get_service},
};
use futures::TryStreamExt as _;
use http::{StatusCode, header::CONTENT_TYPE};
use mime::APPLICATION_OCTET_STREAM;
use reqwest::Client;
use tokio::{fs::File, io::BufReader, net::TcpListener, task::JoinSet};
use tokio_util::{compat::TokioAsyncReadCompatExt as _, io::StreamReader, sync::CancellationToken};
use tower_http::services::ServeFile;
use url::Url;
use weeb::{html::template, net::http::app};

#[tokio::test]
async fn handle_zip_ok() -> anyhow::Result<()> {
    let mut set = JoinSet::new();
    let token = CancellationToken::new();

    let num_chapters = 1 << 1;
    let num_images = 1 << 1;

    let (client, api_url) = api_serve(&mut set, token.clone(), num_chapters, num_images).await?;
    let (client, mut url) = serve(&mut set, token.clone(), client).await?;

    let series_url = api_url.join("series/1")?;
    url.query_pairs_mut()
        .append_pair("series_url", series_url.as_str());

    let response = client.get(url).send().await?;
    assert_eq!(response.status(), StatusCode::OK);
    let headers = response.headers();
    let content_type = headers
        .get(CONTENT_TYPE)
        .context("Missing content-type header")?;
    assert_eq!(content_type, APPLICATION_OCTET_STREAM.as_ref());

    // write to a temp file and then parse it
    {
        let mut file = File::create("test.zip").await?;
        let mut stream = StreamReader::new(response.bytes_stream().map_err(std::io::Error::other));

        assert!(tokio::io::copy(&mut stream, &mut file).await? > 0);
    };

    let mut num_files = 0;
    let archive = File::open("test.zip").await?;
    let archive = BufReader::new(archive).compat();
    let reader = ZipFileReader::new(archive).await?;
    for index in 0..reader.file().entries().len() {
        let entry = reader
            .file()
            .entries()
            .get(index)
            .ok_or_else(|| anyhow::format_err!("No entry found"))?;
        assert_eq!(entry.dir()?, false);
        // open the zip as file
        num_files += 1;
        // Read the content of the zip file
    }
    assert_eq!(num_files, num_chapters);

    token.cancel();
    for res in set.join_all().await {
        res?
    }
    assert!(token.is_cancelled());
    Ok(())
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
