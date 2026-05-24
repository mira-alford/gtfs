pub mod bridge;
pub mod db;
pub mod gtfs;
pub mod vars;

use anyhow::Result;
use prost::Message;
use reqwest::Client;
use std::env;
use std::time::Duration;
use tokio_cron_scheduler::{Job, JobScheduler};
use tracing::{debug, error, info};
use tracing_subscriber::{EnvFilter, field::MakeExt};

use crate::db::queries;
use crate::{db::Db, gtfs::load_static_gtfs};

pub mod transit_realtime {
    include!(concat!(env!("OUT_DIR"), "/transit_realtime.rs"));
}

#[derive(Clone)]
pub struct State {
    db: Db,
    client: Client,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .map_fmt_fields(|f| f.debug_alt())
        .init();

    // Set up the DB connection pool
    let mut db = Db::connect().await?;
    db.run_migrations().await?;

    // setup_static_poll_schedule().await.unwrap();

    // Set up the reqwest client
    let client = Client::new();

    let state = State { db, client };

    // fire poll once immediately on boot
    static_poll(state.clone()).await?;

    setup_static_poll_schedule(state.clone()).await?;

    loop {
        if let Err(e) = dynamic_poll().await {
            error!(e=?e);
        }
        tokio::time::sleep(Duration::from_mins(1)).await;
    }
}

async fn setup_static_poll_schedule(state: State) -> Result<()> {
    let fetch_hour_of_day: u32 = env::var("STATIC_FETCH_TIME_OF_DAY")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(3);

    let sched = JobScheduler::new().await?;
    sched
        .add(Job::new_async(
            format!("0 0 {} * * *", fetch_hour_of_day),
            {
                move |_uuid, _l| {
                    let state = state.clone();
                    Box::pin(async move {
                        if let Err(e) = static_poll(state).await {
                            eprintln!("Unable to poll static data: {e}");
                        }
                    })
                }
            },
        )?)
        .await?;
    sched.start().await?;

    Ok(())
}

async fn static_poll(state: State) -> Result<()> {
    // do the stuff
    let last_update = queries::get_feed_last_update("SEQ".into(), &state.db.0).await?;

    let gtfs = load_static_gtfs("./seq_gtfs.zip".to_owned(), last_update).await?;

    if let Some(gtfs) = gtfs {
        gtfs.insert_db(state.db.clone()).await?;
    }

    Ok(())
}

async fn dynamic_poll() -> Result<()> {
    let pb = reqwest::get("https://gtfsrt.api.translink.com.au/api/realtime/SEQ/TripUpdates")
        .await?
        .bytes()
        .await?;

    let message = transit_realtime::FeedMessage::decode(pb)?;

    // let time = message.header.timestamp() as i64;
    // let time = DateTime::from_timestamp(time, 0);

    // debug!(header=?message.header);
    // debug!(time=?time);

    for entity in message.entity {
        let Some(trip) = entity.trip_update.as_ref() else {
            continue;
        };

        debug!(trip=?trip);
    }

    info!("Polled");

    Ok(())
}
