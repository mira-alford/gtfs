//! DB Queries
//!
//! A whole bunch of internal queries for the db.
//! All the SQL should be in here.

use super::types::*;
use chrono::NaiveDateTime;
use sqlx::{PgConnection, PgPool, QueryBuilder};

// pub async fn insert_agency(agency: &Agency, pool: &mut PgConnection) -> Result<(), sqlx::Error> {
//     sqlx::query!(
//         r#"
//         INSERT INTO agency (agency_name, agency_url, agency_timezone, agency_lang, agency_phone)
//         VALUES ($1, $2, $3, $4, $5)
//         "#,
//         agency.agency_name,
//         agency.agency_url,
//         agency.agency_timezone,
//         agency.agency_lang,
//         agency.agency_phone
//     )
//     .execute(pool)
//     .await?;
//     Ok(())
// }

// pub async fn insert_stop(stop: &Stop, pool: &mut PgConnection) -> Result<(), sqlx::Error> {
//     sqlx::query!(
//         r#"
//         INSERT INTO stops (
//             stop_id, stop_code, stop_name, stop_desc, stop_lat, stop_lon,
//             zone_id, stop_url, location_type, parent_station, platform_code
//         )
//         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
//         "#,
//         stop.stop_id,
//         stop.stop_code,
//         stop.stop_name,
//         stop.stop_desc,
//         stop.stop_lat,
//         stop.stop_lon,
//         stop.zone_id,
//         stop.stop_url,
//         stop.location_type,
//         stop.parent_station,
//         stop.platform_code
//     )
//     .execute(pool)
//     .await?;
//     Ok(())
// }

// pub async fn insert_route(route: &Route, pool: &mut PgConnection) -> Result<(), sqlx::Error> {
//     sqlx::query!(
//         r#"
//         INSERT INTO routes (
//             route_id, route_short_name, route_long_name, route_desc, route_type,
//             route_url, route_color, route_text_color
//         )
//         VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
//         "#,
//         route.route_id,
//         route.route_short_name,
//         route.route_long_name,
//         route.route_desc,
//         route.route_type,
//         route.route_url,
//         route.route_color,
//         route.route_text_color
//     )
//     .execute(pool)
//     .await?;
//     Ok(())
// }

// pub async fn insert_trip(trip: &Trip, pool: &mut PgConnection) -> Result<(), sqlx::Error> {
//     sqlx::query!(
//         r#"
//         INSERT INTO trips (
//             route_id, service_id, trip_id, trip_headsign,
//             direction_id, block_id, shape_id
//         )
//         VALUES ($1,$2,$3,$4,$5,$6,$7)
//         "#,
//         trip.route_id,
//         trip.service_id,
//         trip.trip_id,
//         trip.trip_headsign,
//         trip.direction_id,
//         trip.block_id,
//         trip.shape_id
//     )
//     .execute(pool)
//     .await?;
//     Ok(())
// }

// pub async fn insert_stop_time(
//     stop_time: &StopTime,
//     pool: &mut PgConnection,
// ) -> Result<(), sqlx::Error> {
//     sqlx::query!(
//         r#"
//         INSERT INTO stop_times (
//             trip_id, arrival_time, departure_time, stop_id,
//             stop_sequence, pickup_type, drop_off_type
//         )
//         VALUES ($1,$2,$3,$4,$5,$6,$7)
//         "#,
//         stop_time.trip_id,
//         stop_time.arrival_time,
//         stop_time.departure_time,
//         stop_time.stop_id,
//         stop_time.stop_sequence,
//         stop_time.pickup_type,
//         stop_time.drop_off_type
//     )
//     .execute(pool)
//     .await?;
//     Ok(())
// }

// pub async fn insert_calendar(
//     calendar: &Calendar,
//     pool: &mut PgConnection,
// ) -> Result<(), sqlx::Error> {
//     sqlx::query!(
//         r#"
//         INSERT INTO calendar (
//             service_id, monday, tuesday, wednesday, thursday,
//             friday, saturday, sunday, start_date, end_date
//         )
//         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
//         "#,
//         calendar.service_id,
//         calendar.monday,
//         calendar.tuesday,
//         calendar.wednesday,
//         calendar.thursday,
//         calendar.friday,
//         calendar.saturday,
//         calendar.sunday,
//         calendar.start_date,
//         calendar.end_date
//     )
//     .execute(pool)
//     .await?;
//     Ok(())
// }

// pub async fn insert_calendar_date(
//     cd: &CalendarDate,
//     pool: &mut PgConnection,
// ) -> Result<(), sqlx::Error> {
//     sqlx::query!(
//         r#"
//         INSERT INTO calendar_dates (
//             service_id, date, exception_type
//         )
//         VALUES ($1,$2,$3)
//         "#,
//         cd.service_id,
//         cd.date,
//         cd.exception_type
//     )
//     .execute(pool)
//     .await?;
//     Ok(())
// }

// pub async fn insert_shape(shape: &Shape, pool: &mut PgConnection) -> Result<(), sqlx::Error> {
//     sqlx::query!(
//         r#"
//         INSERT INTO shapes (
//             shape_id, shape_pt_lat, shape_pt_lon, shape_pt_sequence
//         )
//         VALUES ($1,$2,$3,$4)
//         "#,
//         shape.shape_id,
//         shape.shape_pt_lat,
//         shape.shape_pt_lon,
//         shape.shape_pt_sequence
//     )
//     .execute(pool)
//     .await?;
//     Ok(())
// }

// pub async fn insert_shapes(
//     shapes: &Vec<Shape>,
//     pool: &mut PgConnection,
// ) -> Result<(), sqlx::Error> {
//     // let (ids, pt_lats, pt_lons, pt_sequences): (_, _, _, _) = (
//     //     shapes.iter().map(|s| s.shape_id.as_str()).collect_vec(),
//     //     shapes.iter().map(|s| s.shape_pt_lat).collect_vec(),
//     //     shapes.iter().map(|s| s.shape_pt_lon).collect_vec(),
//     //     shapes.iter().map(|s| s.shape_pt_sequence).collect_vec(),
//     // );
//     // sqlx::query!(
//     //     r#"
//     //     INSERT INTO shapes (
//     //         shape_id, shape_pt_lat, shape_pt_lon, shape_pt_sequence
//     //     ) SELECT * FROM UNNEST(
//     //         $1::text[],
//     //         $2::float8[],
//     //         $3::float8[],
//     //         $4::integer[]
//     //     )
//     //     "#,
//     //     ids.as_slice(),
//     //     &pt_lats,
//     //     &pt_lons,
//     //     &pt_sequences
//     // )
//     // .execute(pool)
//     // .await?;
//     // Ok(())
//     let mut qb = QueryBuilder::new(
//         "INSERT INTO shapes (shape_id, shape_pt_lat, shape_pt_lon, shape_pt_sequence) ",
//     );

//     qb.push_values(shapes.iter(), |mut b, s| {
//         b.push_bind(&s.shape_id)
//             .push_bind(s.shape_pt_lat)
//             .push_bind(s.shape_pt_lon)
//             .push_bind(s.shape_pt_sequence);
//     });

//     qb.build().execute(pool).await?;

//     Ok(())
// }

// pub async fn insert_feed_info(feed: &FeedInfo, pool: &mut PgConnection) -> Result<(), sqlx::Error> {
//     sqlx::query!(
//         r#"
//         INSERT INTO feed_info (
//             feed_publisher_name, feed_publisher_url,
//             feed_lang, feed_start_date, feed_end_date
//         )
//         VALUES ($1,$2,$3,$4,$5)
//         "#,
//         feed.feed_publisher_name,
//         feed.feed_publisher_url,
//         feed.feed_lang,
//         feed.feed_start_date,
//         feed.feed_end_date
//     )
//     .execute(pool)
//     .await?;
//     Ok(())
// }

// pub async fn insert_last_update(
//     last_update: &LastUpdate,
//     pool: &mut PgConnection,
// ) -> Result<(), sqlx::Error> {
//     sqlx::query!(
//         r#"
//         INSERT INTO last_update (
//             feed_region, feed_last_update
//         )
//         VALUES ($1,$2)
//         "#,
//         last_update.feed_region,
//         last_update.feed_last_update
//     )
//     .execute(pool)
//     .await?;
//     Ok(())
// }

pub async fn get_feed_last_update(
    feed_region: String,
    pool: &PgPool,
) -> Result<Option<NaiveDateTime>, sqlx::Error> {
    let row = sqlx::query!(
        r#"
        SELECT feed_last_update
        FROM last_update
        WHERE feed_region = $1
        "#,
        feed_region
    )
    .fetch_optional(pool)
    .await?;

    // Default to 1970-01-01 00:00:00 (the "zero" NaiveDateTime)
    Ok(row.map(|r| r.feed_last_update))
}
