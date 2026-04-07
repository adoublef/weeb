use crate::encoding::html::{anchors, images};
use anyhow::anyhow;
use async_stream::try_stream;
use async_zip::{Compression, ZipEntryBuilder, base::write::ZipFileWriter};
use bytes::Bytes;
use chrono::Utc;
use futures_util::{Stream, StreamExt as _, TryStreamExt};
use http_body_util::{BodyDataStream, Limited};
use mimetype_detector::{detect, equals_any};
use tokio::{
    io::{AsyncRead, copy, duplex},
    pin, spawn,
    sync::mpsc,
    task::{JoinSet, spawn_blocking},
};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::{
    compat::{FuturesAsyncReadCompatExt, FuturesAsyncWriteCompatExt},
    io::{ReaderStream, StreamReader, SyncIoBridge},
};
use url::Url;

const DEFAULT_CHAN_BUF_SIZE: usize = 1;
const DEFAULT_CONCURRENT_LIMIT: usize = 1;
const DEFAULT_MAX_BUF_SIZE: usize = 4 << 10;

#[derive(Debug, Clone, Default)]
pub struct HttpClient(pub reqwest::Client);

impl HttpClient {
    fn chapter_urls(&self, mut series_url: Url) -> impl Stream<Item = anyhow::Result<Url>> {
        let (tx, rx) = mpsc::channel(DEFAULT_CHAN_BUF_SIZE);
        let fut = spawn({
            let client = self.0.clone();
            async move {
                series_url
                    .path_segments_mut()
                    .map_err(|_| anyhow!("Invalid path segments"))?
                    .push("full-chapter-list");

                let response = client
                    .get(series_url)
                    .send()
                    .await?
                    .error_for_status()?
                    .bytes_stream();
                let stream = StreamReader::new(response.map_err(std::io::Error::other));
                let fut = spawn_blocking(move || {
                    let stream = SyncIoBridge::new(stream);
                    for res in anchors(stream) {
                        let res = res?; // validate url parts
                        tx.blocking_send(res)?;
                    }
                    anyhow::Ok(())
                });
                anyhow::Ok(fut.await??)
            }
        });

        let stream = try_stream! {
            let mut stream = ReceiverStream::new(rx);
            while let Some(res) = stream.next().await {
                yield res
            }
            fut.await??;
        };
        stream
    }

    async fn image_urls(
        &self,
        mut chapter_url: Url,
    ) -> anyhow::Result<impl Stream<Item = anyhow::Result<Url>>> {
        chapter_url
            .path_segments_mut()
            .map_err(|_| anyhow!("Invalid path segments"))?
            .push("images");

        let response = self
            .0
            .get(chapter_url)
            .send()
            .await?
            .error_for_status()?
            .bytes_stream();
        let stream = StreamReader::new(response.map_err(std::io::Error::other));

        let (tx, rx) = mpsc::channel(DEFAULT_CHAN_BUF_SIZE);
        let fut = spawn_blocking(move || {
            let stream = SyncIoBridge::new(stream);
            for res in images(stream) {
                let res = res?; // validate url parts
                tx.blocking_send(res)?;
            }
            anyhow::Ok(())
        });

        let stream = try_stream! {
            let mut stream = ReceiverStream::new(rx);
            while let Some(res) = stream.next().await {
                yield res
            }
            fut.await??;
        };
        Ok(stream)
    }

    async fn image_reader(&self, image_url: Url) -> anyhow::Result<impl AsyncRead> {
        let response = self.0.get(image_url).send().await?.error_for_status()?;

        let content_length = response.content_length().unwrap_or(0);

        let body = reqwest::Body::from(response);
        let limited_body = Limited::new(body, content_length as usize);
        let stream = BodyDataStream::new(limited_body);
        let response_stream = StreamReader::new(stream.map_err(std::io::Error::other));

        Ok(response_stream)
    }
}

#[derive(Clone, Debug, Default)]
pub struct Handler {
    // client: reqwest::Client,
    http_client: HttpClient,
}

impl Handler {
    pub fn series_stream(
        &self,
        series_url: Url,
        deflate: bool,
    ) -> impl Stream<Item = anyhow::Result<Bytes>> + 'static {
        let mut set = JoinSet::new();

        let (tx, chapter_urls) = mpsc::channel(DEFAULT_CHAN_BUF_SIZE);
        set.spawn({
            let client = self.http_client.clone();
            async move {
                let stream = client.chapter_urls(series_url);
                pin!(stream);
                // let mut stream: impl Stream<Item = Result<Url, Error>>
                while let Some(res) = stream.next().await {
                    let res = res?;
                    tx.send(res).await?;
                }
                anyhow::Ok(())
            }
        });

        let (rx, tx) = duplex(DEFAULT_MAX_BUF_SIZE);
        set.spawn({
            let this = self.clone();
            async move {
                let compress = if deflate {
                    Compression::Deflate
                } else {
                    Compression::Stored
                };
                let mut wri = ZipFileWriter::with_tokio(tx).force_zip64();

                let mut stream = ReceiverStream::new(chapter_urls).enumerate();
                while let Some((ix, chapter_url)) = stream.next().await {
                    let chapter_stream = StreamReader::new(
                        this.chapter_stream(chapter_url, deflate)
                            .map_err(std::io::Error::other),
                    );
                    pin!(chapter_stream); // don't like this, look into Unpin without needing to use pin
                    let mut chapter_entry = wri
                        .write_entry_stream(
                            ZipEntryBuilder::new(format!("chapter-{ix}.zip").into(), compress)
                                .last_modification_date(Utc::now().into()),
                        )
                        .await?
                        .compat_write();

                    copy(&mut chapter_stream, &mut chapter_entry).await?;
                    chapter_entry.into_inner().close().await?;
                }

                anyhow::Ok(wri.close().await.map(|_| ())?)
            }
        });

        let stream = try_stream! {
            let mut stream = ReaderStream::new(rx);
            while let Some(res) = stream.next().await {
                yield res?
            }
            while let Some(res) = set.join_next().await {
                res??;
            }
        };
        stream
    }

    fn chapter_stream(
        &self,
        chapter_url: Url,
        deflate: bool,
    ) -> impl Stream<Item = anyhow::Result<Bytes>> {
        let mut set = JoinSet::new();

        let (tx, image_urls) = mpsc::channel(DEFAULT_CHAN_BUF_SIZE);
        set.spawn({
            let client = self.http_client.clone();
            async move {
                let stream = client.image_urls(chapter_url).await?;
                pin!(stream);
                while let Some(res) = stream.next().await {
                    let res = res?;
                    tx.send(res).await?;
                }
                anyhow::Ok(())
            }
        });

        // copy all images into buffers
        let (tx, image_bufs) = mpsc::channel(DEFAULT_CHAN_BUF_SIZE);
        set.spawn({
            let client = self.http_client.clone();
            async move {
                anyhow::Ok(
                    ReceiverStream::new(image_urls)
                        .map(anyhow::Ok)
                        .try_for_each_concurrent(DEFAULT_CONCURRENT_LIMIT, |image_url| {
                            let client = client.clone();
                            let tx = tx.clone();
                            async move {
                                let mut reader = client.image_reader(image_url).await?;
                                // pin!(reader);

                                let mut buf = Vec::new();
                                copy(&mut reader, &mut buf).await?;

                                // TODO: i want to detect _before_ reading the whole content since we only need
                                let mime_type = detect(&buf).mime();
                                if !equals_any(
                                    mime_type,
                                    &[
                                        mime::IMAGE_JPEG.essence_str(),
                                        mime::IMAGE_PNG.essence_str(),
                                    ],
                                ) {
                                    return Err(anyhow!("Invalid mime type"));
                                };
                                anyhow::Ok(tx.send(buf).await?)
                            }
                        })
                        .await?,
                )
            }
        });

        let (rx, tx) = duplex(DEFAULT_MAX_BUF_SIZE);
        set.spawn({
            async move {
                let compress = if deflate {
                    Compression::Deflate
                } else {
                    Compression::Stored
                };
                let mut wri = ZipFileWriter::with_tokio(tx).force_zip64();

                let mut stream = ReceiverStream::new(image_bufs).enumerate();
                while let Some((ix, image_buf)) = stream.next().await {
                    // Compat<&[u8]>
                    let mut image_entry = wri
                        .write_entry_stream(
                            ZipEntryBuilder::new(format!("image-{ix}.png").into(), compress)
                                .uncompressed_size(image_buf.len() as u64) // 86387
                                .last_modification_date(Utc::now().into()),
                        )
                        .await?
                        .compat_write();

                    copy(&mut image_buf.compat(), &mut image_entry).await?;
                    image_entry.into_inner().close().await?;
                }

                anyhow::Ok(wri.close().await.map(|_| ())?)
            }
        });

        let chapter = try_stream! {
            let mut stream = ReaderStream::new(rx);
            while let Some(res) = stream.next().await {
                yield res?
            }
            while let Some(res) = set.join_next().await {
                res??;
            }
        };
        chapter
    }
}
