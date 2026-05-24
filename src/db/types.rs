//! DB Types
//!
//! Types for database operations.
//! Directly map to schema tables.

use chrono::{NaiveDate, NaiveDateTime, Timelike, Utc};
use sqlx::{
    FromRow, PgConnection, Postgres, QueryBuilder, Transaction, postgres::types::PgInterval,
    query_builder::Separated,
};

use crate::db;

pub trait InsertDB: Sized + Send + Sync {
    fn insert_into(qb: &mut QueryBuilder<Postgres>);
    fn value(self, qb: &mut Separated<Postgres, &'static str>);
}

/// Generic insert helper
pub async fn insert_one<T: InsertDB>(
    item: T,
    tx: &mut Transaction<'_, Postgres>,
) -> Result<(), sqlx::Error> {
    let mut qb = QueryBuilder::<Postgres>::new("");

    T::insert_into(&mut qb);

    qb.push(" VALUES (");
    item.value(&mut qb.separated(","));
    qb.push(")");

    qb.build().execute(&mut **tx).await?;

    Ok(())
}

/// Generic bulk insert helper
pub async fn insert_many<T: InsertDB>(
    items: Vec<T>,
    tx: &mut Transaction<'_, Postgres>,
) -> Result<(), sqlx::Error> {
    if items.is_empty() {
        return Ok(());
    }

    let mut qb = QueryBuilder::<Postgres>::new("");

    T::insert_into(&mut qb);

    qb.push_values(items.into_iter(), |mut b, item| {
        item.value(&mut b);
    });

    qb.build().execute(&mut **tx).await?;

    Ok(())
}

/// Representation of agency table rows
#[derive(Debug, FromRow, PartialEq, Eq)]
pub struct Agency {
    pub agency_name: String,
    pub agency_url: String,
    pub agency_timezone: String,
    pub agency_lang: Option<String>,
    pub agency_phone: Option<String>,
}

impl InsertDB for Agency {
    fn insert_into(qb: &mut QueryBuilder<Postgres>) {
        qb.push(
            "INSERT INTO agency \
            (agency_name, agency_url, agency_timezone, agency_lang, agency_phone)",
        );
    }

    fn value(self, qb: &mut Separated<Postgres, &'static str>) {
        qb.push_bind(self.agency_name)
            .push_bind(self.agency_url)
            .push_bind(self.agency_timezone)
            .push_bind(self.agency_lang)
            .push_bind(self.agency_phone);
    }
}

/// Representation of stops table rows
#[derive(Debug, FromRow, PartialEq)]
pub struct Stop {
    pub stop_id: String,
    pub stop_code: Option<String>,
    pub stop_name: Option<String>,
    pub stop_desc: Option<String>,
    pub stop_lat: Option<f64>,
    pub stop_lon: Option<f64>,
    pub zone_id: Option<String>,
    pub stop_url: Option<String>,
    pub location_type: Option<i32>,
    pub parent_station: Option<String>,
    pub platform_code: Option<String>,
}

impl InsertDB for Stop {
    fn insert_into(qb: &mut QueryBuilder<Postgres>) {
        qb.push(
            "INSERT INTO stops (
                stop_id, stop_code, stop_name, stop_desc,
                stop_lat, stop_lon, zone_id, stop_url,
                location_type, parent_station, platform_code
            )",
        );
    }

    fn value(self, qb: &mut Separated<Postgres, &'static str>) {
        qb.push_bind(self.stop_id)
            .push_bind(self.stop_code)
            .push_bind(self.stop_name)
            .push_bind(self.stop_desc)
            .push_bind(self.stop_lat)
            .push_bind(self.stop_lon)
            .push_bind(self.zone_id)
            .push_bind(self.stop_url)
            .push_bind(self.location_type)
            .push_bind(self.parent_station)
            .push_bind(self.platform_code);
    }
}

/// Representation of routes table rows
#[derive(Debug, FromRow, PartialEq, Eq)]
pub struct Route {
    pub route_id: String,
    pub route_short_name: Option<String>,
    pub route_long_name: Option<String>,
    pub route_desc: Option<String>,
    pub route_type: i32,
    pub route_url: Option<String>,
    pub route_color: Option<String>,
    pub route_text_color: Option<String>,
}

impl InsertDB for Route {
    fn insert_into(qb: &mut QueryBuilder<Postgres>) {
        qb.push(
            "INSERT INTO routes (
                route_id, route_short_name, route_long_name,
                route_desc, route_type, route_url,
                route_color, route_text_color
            )",
        );
    }

    fn value(self, qb: &mut Separated<Postgres, &'static str>) {
        qb.push_bind(self.route_id)
            .push_bind(self.route_short_name)
            .push_bind(self.route_long_name)
            .push_bind(self.route_desc)
            .push_bind(self.route_type)
            .push_bind(self.route_url)
            .push_bind(self.route_color)
            .push_bind(self.route_text_color);
    }
}

/// Representation of trips table rows
#[derive(Debug, FromRow, PartialEq, Eq)]
pub struct Trip {
    pub route_id: String,
    pub service_id: String,
    pub trip_id: String,
    pub trip_headsign: Option<String>,
    pub direction_id: Option<bool>,
    pub block_id: Option<String>,
    pub shape_id: Option<String>,
}

impl InsertDB for Trip {
    fn insert_into(qb: &mut QueryBuilder<Postgres>) {
        qb.push(
            "INSERT INTO trips (
                route_id, service_id, trip_id,
                trip_headsign, direction_id,
                block_id, shape_id
            )",
        );
    }

    fn value(self, qb: &mut Separated<Postgres, &'static str>) {
        qb.push_bind(self.route_id)
            .push_bind(self.service_id)
            .push_bind(self.trip_id)
            .push_bind(self.trip_headsign)
            .push_bind(self.direction_id)
            .push_bind(self.block_id)
            .push_bind(self.shape_id);
    }
}

/// Representation of stop_times table rows
#[derive(Debug, FromRow, PartialEq, Eq)]
pub struct StopTime {
    pub trip_id: String,
    pub arrival_time: Option<PgInterval>,
    pub departure_time: PgInterval,
    pub stop_id: String,
    pub stop_sequence: i32,
    pub pickup_type: i32,
    pub drop_off_type: i32,
}

impl InsertDB for StopTime {
    fn insert_into(qb: &mut QueryBuilder<Postgres>) {
        qb.push(
            "INSERT INTO stop_times (
                trip_id, arrival_time, departure_time,
                stop_id, stop_sequence,
                pickup_type, drop_off_type
            )",
        );
    }

    fn value(self, qb: &mut Separated<Postgres, &'static str>) {
        qb.push_bind(self.trip_id)
            .push_bind(self.arrival_time)
            .push_bind(self.departure_time)
            .push_bind(self.stop_id)
            .push_bind(self.stop_sequence)
            .push_bind(self.pickup_type)
            .push_bind(self.drop_off_type);
    }
}

/// Representation of calendar table rows
#[derive(Debug, FromRow, PartialEq, Eq)]
pub struct Calendar {
    pub service_id: String,
    pub monday: bool,
    pub tuesday: bool,
    pub wednesday: bool,
    pub thursday: bool,
    pub friday: bool,
    pub saturday: bool,
    pub sunday: bool,
    pub start_date: NaiveDate,
    pub end_date: NaiveDate,
}

impl InsertDB for Calendar {
    fn insert_into(qb: &mut QueryBuilder<Postgres>) {
        qb.push(
            "INSERT INTO calendar (
                service_id, monday, tuesday,
                wednesday, thursday, friday,
                saturday, sunday,
                start_date, end_date
            )",
        );
    }

    fn value(self, qb: &mut Separated<Postgres, &'static str>) {
        qb.push_bind(self.service_id)
            .push_bind(self.monday)
            .push_bind(self.tuesday)
            .push_bind(self.wednesday)
            .push_bind(self.thursday)
            .push_bind(self.friday)
            .push_bind(self.saturday)
            .push_bind(self.sunday)
            .push_bind(self.start_date)
            .push_bind(self.end_date);
    }
}

/// Representation of calendar_date table rows
#[derive(Debug, FromRow, PartialEq, Eq)]
pub struct CalendarDate {
    pub service_id: String,
    pub date: NaiveDate,
    pub exception_type: i32,
}

impl InsertDB for CalendarDate {
    fn insert_into(qb: &mut QueryBuilder<Postgres>) {
        qb.push(
            "INSERT INTO calendar_dates (
                service_id, date, exception_type
            )",
        );
    }

    fn value(self, qb: &mut Separated<Postgres, &'static str>) {
        qb.push_bind(self.service_id)
            .push_bind(self.date)
            .push_bind(self.exception_type);
    }
}

/// Representation of shapes table rows
#[derive(Debug, FromRow, PartialEq)]
pub struct Shape {
    pub shape_id: String,
    pub shape_pt_lat: f64,
    pub shape_pt_lon: f64,
    pub shape_pt_sequence: i32,
}

impl InsertDB for Shape {
    fn insert_into(qb: &mut QueryBuilder<Postgres>) {
        qb.push(
            "INSERT INTO shapes (
                shape_id, shape_pt_lat,
                shape_pt_lon, shape_pt_sequence
            )",
        );
    }

    fn value(self, qb: &mut Separated<Postgres, &'static str>) {
        qb.push_bind(self.shape_id)
            .push_bind(self.shape_pt_lat)
            .push_bind(self.shape_pt_lon)
            .push_bind(self.shape_pt_sequence);
    }
}

/// Representation of feed_info table rows
#[derive(Debug, FromRow, PartialEq, Eq)]
pub struct FeedInfo {
    pub feed_publisher_name: String,
    pub feed_publisher_url: String,
    pub feed_lang: Option<String>,
    pub feed_start_date: Option<NaiveDate>,
    pub feed_end_date: Option<NaiveDate>,
}

impl InsertDB for FeedInfo {
    fn insert_into(qb: &mut QueryBuilder<Postgres>) {
        qb.push(
            "INSERT INTO feed_info (
                feed_publisher_name,
                feed_publisher_url,
                feed_lang,
                feed_start_date,
                feed_end_date
            )",
        );
    }

    fn value(self, qb: &mut Separated<Postgres, &'static str>) {
        qb.push_bind(self.feed_publisher_name)
            .push_bind(self.feed_publisher_url)
            .push_bind(self.feed_lang)
            .push_bind(self.feed_start_date)
            .push_bind(self.feed_end_date);
    }
}

/// Representation of feed_last_update table rows
#[derive(Debug, FromRow, PartialEq, Eq)]
pub struct LastUpdate {
    pub feed_region: String,
    pub feed_last_update: NaiveDateTime,
}

impl LastUpdate {
    pub fn new(feed_region: String) -> LastUpdate {
        LastUpdate {
            feed_region,
            feed_last_update: Utc::now()
                .with_nanosecond(0)
                .unwrap_or(Utc::now())
                .naive_utc(),
        }
    }
}

impl InsertDB for LastUpdate {
    fn insert_into(qb: &mut QueryBuilder<Postgres>) {
        qb.push(
            "INSERT INTO last_update (
                feed_region,
                feed_last_update
            )",
        );
    }

    fn value(self, qb: &mut Separated<Postgres, &'static str>) {
        qb.push_bind(self.feed_region)
            .push_bind(self.feed_last_update);
    }
}
