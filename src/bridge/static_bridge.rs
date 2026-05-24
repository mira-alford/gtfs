use std::time::Duration;

use crate::{
    db::{
        self,
        types::{InsertDB, Shape},
    },
    gtfs::StaticGtfs,
};
use anyhow::{Context, Result, anyhow};
use futures::{StreamExt, future::try_join_all};
use rayon::prelude::*;
use sqlx::postgres::types::PgInterval;
use tokio::{
    sync::mpsc::{UnboundedReceiver, UnboundedSender},
    task::JoinHandle,
};
use tokio_stream::wrappers::ReceiverStream;
use tracing::{debug, info};

#[derive(Debug)]
pub struct GtfsDbModel {
    pub agencies: ReceiverStream<db::types::Agency>,
    pub stops: ReceiverStream<db::types::Stop>,
    pub routes: ReceiverStream<db::types::Route>,
    pub trips: ReceiverStream<db::types::Trip>,
    pub stop_times: ReceiverStream<db::types::StopTime>,
    pub calendar: ReceiverStream<db::types::Calendar>,
    pub calendar_dates: ReceiverStream<db::types::CalendarDate>,
    pub shapes: ReceiverStream<db::types::Shape>,
    pub feed_info: ReceiverStream<db::types::FeedInfo>,
}

fn convert<T, U>(label: &'static str, items: Vec<T>, chunk_size: usize) -> UnboundedReceiver<U>
where
    T: ToDB<U> + Send + Sync + Clone + 'static,
    U: Send + Sync + 'static,
{
    let (sender, receiver): (UnboundedSender<U>, UnboundedReceiver<U>) =
        tokio::sync::mpsc::unbounded_channel();

    tokio::task::spawn_blocking(move || {
        debug!(%label, "Starting DB Conversion.");
        items.chunks(chunk_size).for_each(|chunk| {
            let converted: Vec<_> = chunk
                .par_iter()
                .filter_map(|item| item.clone().to_db().ok())
                .collect();

            for item in converted {
                if sender.send(item).is_err() {
                    break;
                }
            }
        });
        debug!(%label, "Finished DB Conversion.");
    });

    receiver
}

fn convert_shapes<T, U>(
    label: &'static str,
    items: Vec<T>,
    chunk_size: usize,
) -> UnboundedReceiver<Vec<U>>
where
    T: ToDB<U> + Send + Sync + Clone + 'static,
    U: Send + Sync + 'static,
{
    let (sender, receiver): (UnboundedSender<Vec<U>>, UnboundedReceiver<Vec<U>>) =
        tokio::sync::mpsc::unbounded_channel();

    tokio::task::spawn_blocking(move || {
        debug!(%label, "Starting DB Conversion.");
        items.chunks(chunk_size).for_each(|chunk| {
            let converted: Vec<_> = chunk
                .par_iter()
                .filter_map(|item| item.clone().to_db().ok())
                .collect();

            sender.send(converted);
            // for item in converted {
            //     if sender.send(item).is_err() {
            //         break;
            //     }
            // }
        });
        debug!(%label, "Finished DB Conversion.");
    });

    receiver
}

async fn stream_insert<T: InsertDB + Send + Sync + 'static>(
    label: &'static str,
    db: &db::Db,
    mut rx: UnboundedReceiver<T>,
) -> Result<JoinHandle<()>> {
    let mut tx = db.0.begin().await?;
    Ok(tokio::task::spawn(async move {
        info!(%label, "Starting DB insert.");
        while let Some(item) = rx.recv().await {
            item.insert(&mut tx).await.unwrap();
        }
        info!(%label, "Finished DB insert.");
    }))
}

impl StaticGtfs {
    pub async fn insert_db(self, db: db::Db) -> Result<()> {
        async fn barrier(handles: &mut Vec<JoinHandle<()>>) -> Result<Vec<()>> {
            Ok(try_join_all(std::mem::take(handles)).await?)
        }

        async fn bridge<U: InsertDB + 'static, T: Clone + Send + Sync + ToDB<U>>(
            futures: &mut Vec<JoinHandle<()>>,
            db: &db::Db,
            label: &'static str,
            input: Option<Result<Vec<T>, gtfs_structures::Error>>,
        ) -> Result<()>
        where
            T: Send + 'static,
        {
            let Some(data) = input else {
                return Ok(());
            };

            futures.push(stream_insert(label, db, convert(label, data?, 1024)).await?);

            Ok(())
        }

        async fn bridge_shapes<T: Clone + Send + Sync + ToDB<Shape>>(
            futures: &mut Vec<JoinHandle<()>>,
            db: &db::Db,
            label: &'static str,
            input: Option<Result<Vec<T>, gtfs_structures::Error>>,
        ) -> Result<()>
        where
            T: Send + 'static,
        {
            let Some(data) = input else {
                return Ok(());
            };

            futures.push(stream_insert(label, db, convert_shapes(label, data?, 1024)).await?);

            Ok(())
        }

        info!("Beginning Static Bridge");

        let mut futures = Vec::new();

        // Phase 1:
        // agencies, calendar, shapes, feedinfo
        info!("Starting Static Bridge Phase 1");
        bridge(&mut futures, &db, "agencies", Some(self.raw_gtfs.agencies)).await?;
        bridge(&mut futures, &db, "calendar", self.raw_gtfs.calendar).await?;
        bridge_shapes(&mut futures, &db, "shapes", self.raw_gtfs.shapes).await?;
        bridge(&mut futures, &db, "feed_info", self.raw_gtfs.feed_info).await?;
        barrier(&mut futures).await?;
        info!("Completed Static Bridge Phase 1");

        // Phase 2:
        // stops, routes, calendar_dates
        info!("Starting Static Bridge Phase 2");
        bridge(&mut futures, &db, "stops", Some(self.raw_gtfs.stops)).await?;
        bridge(&mut futures, &db, "routes", Some(self.raw_gtfs.routes)).await?;
        bridge(
            &mut futures,
            &db,
            "calendar_dates",
            self.raw_gtfs.calendar_dates,
        )
        .await?;
        barrier(&mut futures).await?;
        info!("Completed Static Bridge Phase 2");

        // Phase 3:
        // trips
        info!("Starting Static Bridge Phase 3");
        bridge(&mut futures, &db, "trips", Some(self.raw_gtfs.trips)).await?;
        barrier(&mut futures).await?;
        info!("Completed Static Bridge Phase 3");

        // Phase 4:
        // update the last updated entry only after everything succeeds
        info!("Starting Static Bridge Phase 4");
        let mut tx = db.0.begin().await?;
        self.last_update.insert(&mut tx).await?;
        tx.commit().await.ok();
        info!("Completed Static Bridge Phase 4");

        info!("Ending Static Bridge");
        Ok(())
    }
}

pub trait ToDB<T>: Send {
    fn to_db(self) -> Result<T>;
}

// Just a ton of to_db implementations beyond this point:

impl ToDB<bool> for gtfs_structures::DirectionType {
    fn to_db(self) -> Result<bool> {
        Ok(matches!(self, gtfs_structures::DirectionType::Inbound))
    }
}

impl ToDB<i32> for gtfs_structures::LocationType {
    fn to_db(self) -> Result<i32> {
        Ok(match self {
            gtfs_structures::LocationType::StopPoint => 0,
            gtfs_structures::LocationType::StopArea => 1,
            gtfs_structures::LocationType::StationEntrance => 2,
            gtfs_structures::LocationType::GenericNode => 3,
            gtfs_structures::LocationType::BoardingArea => 4,
            gtfs_structures::LocationType::Unknown(i) => i,
        }
        .into())
    }
}

impl ToDB<i32> for gtfs_structures::RouteType {
    fn to_db(self) -> Result<i32> {
        Ok(match self {
            gtfs_structures::RouteType::Tramway => 0,
            gtfs_structures::RouteType::Subway => 1,
            gtfs_structures::RouteType::Rail => 2,
            gtfs_structures::RouteType::Bus => 3,
            gtfs_structures::RouteType::Ferry => 4,
            gtfs_structures::RouteType::CableCar => 5,
            gtfs_structures::RouteType::Gondola => 6,
            gtfs_structures::RouteType::Funicular => 7,
            gtfs_structures::RouteType::Coach => 8,
            gtfs_structures::RouteType::Air => 9,
            gtfs_structures::RouteType::Taxi => 10,
            gtfs_structures::RouteType::Other(i) => i,
        }
        .into())
    }
}

impl ToDB<PgInterval> for u32 {
    fn to_db(self) -> Result<PgInterval> {
        PgInterval::try_from(Duration::from_secs(self.into()))
            .map_err(|_| anyhow!("Failed to parse PgInterval"))
    }
}

impl ToDB<i32> for gtfs_structures::PickupDropOffType {
    fn to_db(self) -> Result<i32> {
        Ok(match self {
            gtfs_structures::PickupDropOffType::Regular => 0,
            gtfs_structures::PickupDropOffType::NotAvailable => 1,
            gtfs_structures::PickupDropOffType::ArrangeByPhone => 2,
            gtfs_structures::PickupDropOffType::CoordinateWithDriver => 3,
            gtfs_structures::PickupDropOffType::Unknown(i) => i,
        }
        .into())
    }
}

impl ToDB<i32> for gtfs_structures::Exception {
    fn to_db(self) -> Result<i32> {
        Ok(match self {
            gtfs_structures::Exception::Added => 1,
            gtfs_structures::Exception::Deleted => 2,
        })
    }
}

impl ToDB<db::types::Trip> for gtfs_structures::RawTrip {
    fn to_db(self) -> Result<db::types::Trip> {
        Ok(db::types::Trip {
            trip_id: self.id,
            service_id: self.service_id,
            route_id: self.route_id,
            trip_headsign: self.trip_headsign,
            direction_id: self.direction_id.map(|d| d.to_db()).transpose()?,
            block_id: self.block_id,
            shape_id: self.shape_id,
        })
    }
}

impl ToDB<db::types::StopTime> for gtfs_structures::RawStopTime {
    fn to_db(self) -> Result<db::types::StopTime> {
        Ok(db::types::StopTime {
            trip_id: self.trip_id,
            arrival_time: self.arrival_time.map(|t| t.to_db()).transpose()?,
            departure_time: self
                .departure_time
                .context("Missing departure time")?
                .to_db()?,
            stop_id: self.stop_id,
            stop_sequence: self.stop_sequence.try_into()?,
            pickup_type: self.pickup_type.to_db()?,
            drop_off_type: self.drop_off_type.to_db()?,
        })
    }
}

impl ToDB<db::types::Agency> for gtfs_structures::Agency {
    fn to_db(self) -> Result<db::types::Agency> {
        Ok(db::types::Agency {
            agency_name: self.name,
            agency_url: self.url,
            agency_timezone: self.timezone,
            agency_lang: self.lang,
            agency_phone: self.phone,
        })
    }
}

impl ToDB<db::types::Stop> for gtfs_structures::Stop {
    fn to_db(self) -> Result<db::types::Stop> {
        Ok(db::types::Stop {
            stop_id: self.id,
            stop_code: self.code,
            stop_name: self.name,
            stop_desc: self.description,
            stop_lat: self.latitude,
            stop_lon: self.longitude,
            zone_id: self.zone_id,
            stop_url: self.url,
            location_type: Some(self.location_type.to_db()?),
            parent_station: self.parent_station,
            platform_code: self.platform_code,
        })
    }
}

impl ToDB<db::types::Route> for gtfs_structures::Route {
    fn to_db(self) -> Result<db::types::Route> {
        Ok(db::types::Route {
            route_id: self.id,
            route_short_name: self.short_name,
            route_long_name: self.long_name,
            route_desc: self.desc,
            route_type: self.route_type.to_db()?,
            route_url: self.url,
            route_color: Some(format!(
                "{:02X}{:02X}{:02X}",
                self.color.r, self.color.g, self.color.b
            )),
            route_text_color: Some(format!(
                "{:02X}{:02X}{:02X}",
                self.text_color.r, self.text_color.g, self.text_color.b
            )),
        })
    }
}

impl ToDB<db::types::Calendar> for gtfs_structures::Calendar {
    fn to_db(self) -> Result<db::types::Calendar> {
        Ok(db::types::Calendar {
            service_id: self.id,
            monday: self.monday,
            tuesday: self.tuesday,
            wednesday: self.wednesday,
            thursday: self.thursday,
            friday: self.friday,
            saturday: self.saturday,
            sunday: self.sunday,
            start_date: self.start_date,
            end_date: self.end_date,
        })
    }
}

impl ToDB<db::types::CalendarDate> for gtfs_structures::CalendarDate {
    fn to_db(self) -> Result<db::types::CalendarDate> {
        Ok(db::types::CalendarDate {
            service_id: self.service_id,
            date: self.date,
            exception_type: self.exception_type.to_db()?,
        })
    }
}

impl ToDB<db::types::Shape> for gtfs_structures::Shape {
    fn to_db(self) -> Result<db::types::Shape> {
        Ok(db::types::Shape {
            shape_id: self.id,
            shape_pt_lat: self.latitude,
            shape_pt_lon: self.longitude,
            shape_pt_sequence: self.sequence.try_into()?,
        })
    }
}

impl ToDB<db::types::FeedInfo> for gtfs_structures::FeedInfo {
    fn to_db(self) -> Result<db::types::FeedInfo> {
        Ok(db::types::FeedInfo {
            feed_publisher_name: self.name,
            feed_publisher_url: self.url,
            feed_lang: Some(self.lang),
            feed_start_date: self.start_date,
            feed_end_date: self.end_date,
        })
    }
}
