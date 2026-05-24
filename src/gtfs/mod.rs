//! GTFS
//!
//! This module handles a few responsibilities:
//! - Loading static gtfs data via gtfs-structures.
//! - Loading real time gtfs data via protobufs.
//! - Cleaning that up and verifying it.

use crate::db::types::static_types::LastUpdate;
use crate::transit_realtime::FeedMessage;
use anyhow::Context;
use anyhow::Result;
use anyhow::bail;
use chrono::DateTime;
use chrono::FixedOffset;
use chrono::NaiveDateTime;
use futures::future::try_join_all;
use gtfs_structures::RawGtfs;
use prost::Message;
use reqwest::{Client, header::LAST_MODIFIED};
use tokio::task::spawn_blocking;
use tracing::{info, instrument};

/// Static GTFS wrapper.
pub struct StaticGtfs {
    pub raw_gtfs: RawGtfs,
    pub last_update: LastUpdate,
}

impl StaticGtfs {
    pub fn new(raw_gtfs: RawGtfs, last_update: LastUpdate) -> StaticGtfs {
        StaticGtfs {
            raw_gtfs,
            last_update,
        }
    }
}

/// Realtime GTFS wrapper. Stores a vec of FeedMessages, as there may be multiple.
pub struct RealtimeGtfs(pub Vec<FeedMessage>);

/// Checks the last-modified header for a url and returns it, or None if the header is not present.
#[instrument]
pub async fn last_modified(url: String, client: &Client) -> Result<Option<DateTime<FixedOffset>>> {
    let response = client.head(url).send().await?;

    let Some(last_modified) = response.headers().get(LAST_MODIFIED) else {
        return Ok(None);
    };
    let last_modified_str = last_modified.to_str()?;
    Ok(Some(DateTime::parse_from_rfc2822(last_modified_str)?))
}

/// Loads a static gtfs feed from the given path, which is either a file or url.
/// With the translink dataset this can take quite a while (~40 seconds on my pc).
#[instrument]
pub async fn load_static_gtfs(
    url: String,
    last_update: Option<NaiveDateTime>,
) -> Result<Option<StaticGtfs>> {
    if let Some(last_update) = last_update
        && !is_url_content_outdated(&url, last_update).await?
    {
        return Ok(None);
    }

    info!("Loading static GTFS. This may take a while.");
    let gtfs = spawn_blocking(move || RawGtfs::new(&url)).await??;
    info!("Finished loading static GTFS");
    Ok(Some(StaticGtfs::new(
        gtfs,
        LastUpdate::new("SEQ".to_owned()),
    )))
}

pub async fn is_url_content_outdated(url: &String, cutoff: NaiveDateTime) -> Result<bool> {
    if !url.starts_with("https://") {
        return Ok(false);
    }

    let client = Client::new();

    let head = client
        .head(url)
        .send()
        .await
        .context("Failed to send HEAD request")?;

    let last_modified = head
        .headers()
        .get(reqwest::header::LAST_MODIFIED)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| DateTime::parse_from_rfc2822(s).ok())
        .map(|dt| dt.naive_utc());

    if let Some(modified) = last_modified {
        return Ok(modified > cutoff);
    }

    bail!("No Last-Modified header found.");
}

/// Loads realtime gtfs updates.
/// Takes a vec of urls as translink dont have one unified feed.
#[instrument]
pub async fn load_realtime_gtfs(urls: Vec<String>) -> Result<RealtimeGtfs> {
    info!("Loading realtime GTFS.");
    let futures = urls.into_iter().map(|url| async {
        let pb = reqwest::get(url).await?.bytes().await?;
        FeedMessage::decode(pb).context("Failed to decode")
    });

    let messages = try_join_all(futures).await?;
    info!("Finished loading realtime GTFS.");
    Ok(RealtimeGtfs(messages))
}
