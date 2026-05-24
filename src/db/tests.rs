//! DB Tests
//!
//! Specifically tests db queries and operations.
//! Does not test a gtfs dataset properly.


#[cfg(test)]
use super::queries::{
    insert_agency, insert_calendar, insert_calendar_date, insert_feed_info, insert_route,
    insert_shape, insert_stop, insert_stop_time, insert_trip,
};
use tracing_test::traced_test;

#[traced_test]
#[sqlx::test(migrator = "super::MIGRATOR")]
async fn test_agency(pool: PgPool) -> sqlx::Result<()> {
    let mut pool = pool.begin().await?;
    let agency = Agency {
        agency_name: "Translink".into(),
        agency_url: "https://translink.com.au/".into(),
        agency_timezone: "Australia/Brisbane".into(),
        agency_lang: Some("en".into()),
        agency_phone: Some("13 12 30".into()),
    };
    insert_agency(&agency, &mut *pool).await?;

    let row = sqlx::query_as!(
        Agency,
        "SELECT * FROM agency WHERE agency_name = $1",
        &agency.agency_name
    )
    .fetch_one(&mut *pool)
    .await?;

    pool.commit().await?;

    assert_eq!(row, agency);
    Ok(())
}

#[traced_test]
#[sqlx::test(migrator = "super::MIGRATOR")]
async fn test_calendar(pool: PgPool) -> sqlx::Result<()> {
    let mut pool = pool.begin().await?;
    let cal = Calendar {
        service_id: "GCLR 24_25-36991".into(),
        monday: true,
        tuesday: true,
        wednesday: true,
        thursday: true,
        friday: false,
        saturday: false,
        sunday: false,
        start_date: NaiveDate::from_yo_opt(2026, 1).unwrap(),
        end_date: NaiveDate::from_yo_opt(2026, 12).unwrap(),
    };
    insert_calendar(&cal, &mut *pool).await?;

    let row = sqlx::query_as!(
        Calendar,
        "SELECT * FROM calendar WHERE service_id = $1",
        &cal.service_id
    )
    .fetch_one(&mut *pool)
    .await?;

    pool.commit().await?;

    assert_eq!(row, cal);
    Ok(())
}

#[traced_test]
#[sqlx::test(migrator = "super::MIGRATOR")]
async fn test_calendar_date(pool: PgPool) -> sqlx::Result<()> {
    let mut pool = pool.begin().await?;
    let cd = CalendarDate {
        service_id: "BCC 25_26-39839".into(),
        date: NaiveDate::from_yo_opt(2026, 1).unwrap(),
        exception_type: 1,
    };
    insert_calendar_date(&cd, &mut *pool).await?;

    let row = sqlx::query_as!(
        CalendarDate,
        "SELECT * FROM calendar_dates WHERE service_id = $1 AND date = $2",
        &cd.service_id,
        cd.date
    )
    .fetch_one(&mut *pool)
    .await?;

    pool.commit().await?;

    assert_eq!(row, cd);
    Ok(())
}

#[traced_test]
#[sqlx::test(migrator = "super::MIGRATOR")]
async fn test_feed_info(pool: PgPool) -> sqlx::Result<()> {
    let mut pool = pool.begin().await?;
    let feed = FeedInfo {
        feed_publisher_name: "Department of Transport and Main Roads - Translink Division".into(),
        feed_publisher_url: "https://www.translink.com.au/".into(),
        feed_lang: Some("en".into()),
        feed_start_date: Some(NaiveDate::from_yo_opt(2026, 1).unwrap()),
        feed_end_date: Some(NaiveDate::from_yo_opt(2026, 100).unwrap()),
    };
    insert_feed_info(&feed, &mut *pool).await?;

    let row = sqlx::query_as!(
        FeedInfo,
        "SELECT * FROM feed_info WHERE feed_publisher_name = $1",
        &feed.feed_publisher_name
    )
    .fetch_one(&mut *pool)
    .await?;

    pool.commit().await?;

    assert_eq!(row, feed);
    Ok(())
}

#[traced_test]
#[sqlx::test(migrator = "super::MIGRATOR")]
async fn test_last_update(pool: PgPool) -> sqlx::Result<()> {
    let mut pool = pool.begin().await?;
    let last_update = LastUpdate {
        feed_region: "SEQ".to_owned(),
        feed_last_update: Utc::now().with_nanosecond(0).unwrap().naive_utc(),
    };
    insert_last_update(&last_update, &mut *pool).await?;

    let row = sqlx::query_as!(
        LastUpdate,
        "SELECT * FROM last_update WHERE feed_region = $1",
        &last_update.feed_region
    )
    .fetch_one(&mut *pool)
    .await?;

    pool.commit().await?;

    assert_eq!(row, last_update);
    Ok(())
}

#[traced_test]
#[sqlx::test(migrator = "super::MIGRATOR")]
async fn test_route(pool: PgPool) -> sqlx::Result<()> {
    let mut pool = pool.begin().await?;
    let route = Route {
        route_id: "19-4158".into(),
        route_short_name: Some("19".into()),
        route_long_name: Some("Salisbury - PA Hospital StationLink".into()),
        route_desc: None,
        route_type: 3,
        route_url: Some("https://jp.translink.com.au/plan-your-journey/timetables/bus/T/19".into()),
        route_color: Some("E463A4".into()),
        route_text_color: Some("000000".into()),
    };
    insert_route(&route, &mut *pool).await?;

    let row = sqlx::query_as!(
        Route,
        "SELECT * FROM routes WHERE route_id = $1",
        &route.route_id
    )
    .fetch_one(&mut *pool)
    .await?;

    pool.commit().await?;

    assert_eq!(row, route);
    Ok(())
}

#[traced_test]
#[sqlx::test(migrator = "super::MIGRATOR")]
async fn test_shape(pool: PgPool) -> sqlx::Result<()> {
    let mut pool = pool.begin().await?;
    let shape = Shape {
        shape_id: "190008".into(),
        shape_pt_lat: -27.553364,
        shape_pt_lon: 153.023933,
        shape_pt_sequence: 10001,
    };
    insert_shape(&shape, &mut *pool).await?;

    let row = sqlx::query_as!(
        Shape,
        "SELECT * FROM shapes WHERE shape_id = $1 AND shape_pt_sequence = $2",
        &shape.shape_id,
        &shape.shape_pt_sequence
    )
    .fetch_one(&mut *pool)
    .await?;

    pool.commit().await?;

    assert_eq!(row, shape);
    Ok(())
}

#[traced_test]
#[sqlx::test(migrator = "super::MIGRATOR")]
async fn test_stop(pool: PgPool) -> sqlx::Result<()> {
    let mut pool = pool.begin().await?;
    let stop = Stop {
        stop_id: "1".into(),
        stop_code: Some("000001".into()),
        stop_name: Some("Herschel Street Stop 1 near North Quay".into()),
        stop_desc: None,
        stop_lat: Some(-27.467834),
        stop_lon: Some(153.019079),
        zone_id: Some("1".into()),
        stop_url: Some("https://translink.com.au/stop/000001/gtfs/".into()),
        location_type: Some(0),
        parent_station: None,
        platform_code: None,
    };
    insert_stop(&stop, &mut *pool).await?;

    let row = sqlx::query_as!(
        Stop,
        "SELECT * FROM stops WHERE stop_id = $1",
        &stop.stop_id
    )
    .fetch_one(&mut *pool)
    .await?;

    pool.commit().await?;

    assert_eq!(row, stop);
    Ok(())
}

#[traced_test]
#[sqlx::test(migrator = "super::MIGRATOR")]
async fn test_trip(pool: PgPool) -> sqlx::Result<()> {
    let mut pool = pool.begin().await?;
    let route = Route {
        route_id: "R600-3454".into(),
        route_short_name: Some("19".into()),
        route_long_name: Some("Salisbury - PA Hospital StationLink".into()),
        route_desc: None,
        route_type: 3,
        route_url: Some("https://jp.translink.com.au/plan-your-journey/timetables/bus/T/19".into()),
        route_color: Some("E463A4".into()),
        route_text_color: Some("000000".into()),
    };
    insert_route(&route, &mut *pool).await?;

    let trip = Trip {
        route_id: "R600-3454".into(),
        service_id: "ATS_KBL 25-38992".into(),
        trip_id: "32324843-ATS_KBL 25-38992".into(),
        trip_headsign: Some("Bowen Hills station".into()),
        direction_id: Some(false),
        block_id: None,
        shape_id: Some("R6000053".into()),
    };
    insert_trip(&trip, &mut *pool).await?;

    let row = sqlx::query_as!(
        Trip,
        "SELECT * FROM trips WHERE trip_id = $1",
        &trip.trip_id
    )
    .fetch_one(&mut *pool)
    .await?;

    pool.commit().await?;

    assert_eq!(row, trip);
    Ok(())
}

#[traced_test]
#[sqlx::test(migrator = "super::MIGRATOR")]
async fn test_stop_time(pool: PgPool) -> sqlx::Result<()> {
    let mut pool = pool.begin().await?;
    let stop = Stop {
        stop_id: "1".into(),
        stop_code: Some("000001".into()),
        stop_name: Some("Herschel Street Stop 1 near North Quay".into()),
        stop_desc: None,
        stop_lat: Some(-27.467834),
        stop_lon: Some(153.019079),
        zone_id: Some("1".into()),
        stop_url: Some("https://translink.com.au/stop/000001/gtfs/".into()),
        location_type: Some(0),
        parent_station: None,
        platform_code: None,
    };
    insert_stop(&stop, &mut *pool).await?;

    let route = Route {
        route_id: "R600-3454".into(),
        route_short_name: Some("19".into()),
        route_long_name: Some("Salisbury - PA Hospital StationLink".into()),
        route_desc: None,
        route_type: 3,
        route_url: Some("https://jp.translink.com.au/plan-your-journey/timetables/bus/T/19".into()),
        route_color: Some("E463A4".into()),
        route_text_color: Some("000000".into()),
    };
    insert_route(&route, &mut *pool).await?;

    let trip = Trip {
        route_id: "R600-3454".into(),
        service_id: "ATS_KBL 25-38992".into(),
        trip_id: "32324843-ATS_KBL 25-38992".into(),
        trip_headsign: Some("Bowen Hills station".into()),
        direction_id: Some(false),
        block_id: None,
        shape_id: Some("R6000053".into()),
    };
    insert_trip(&trip, &mut *pool).await?;

    let stop_time = StopTime {
        trip_id: trip.trip_id.clone(),
        arrival_time: Some(
            TimeDelta::try_minutes(16 * 60 + 50)
                .unwrap()
                .try_into()
                .unwrap(),
        ),
        departure_time: TimeDelta::try_minutes(16 * 60 + 50)
            .unwrap()
            .try_into()
            .unwrap(),
        stop_id: stop.stop_id.clone(),
        stop_sequence: 1,
        pickup_type: 0,
        drop_off_type: 0,
    };
    insert_stop_time(&stop_time, &mut *pool).await?;

    let row: StopTime = sqlx::query_as!(
        StopTime,
        "SELECT * FROM stop_times WHERE trip_id = $1 AND stop_id = $2",
        &stop_time.trip_id,
        &stop_time.stop_id
    )
    .fetch_one(&mut *pool)
    .await?;

    pool.commit().await?;

    assert_eq!(row, stop_time);
    Ok(())
}

#[traced_test]
#[sqlx::test(migrator = "super::MIGRATOR")]
async fn test_get_feed_last_update(pool: PgPool) -> sqlx::Result<()> {
    let mut transaction = pool.begin().await?;
    let expected_last_update = Utc::now().with_nanosecond(0).unwrap().naive_utc();
    let region = "SEQ".to_owned();

    let last_update = LastUpdate {
        feed_region: region.clone(),
        feed_last_update: expected_last_update.clone(),
    };
    insert_last_update(&last_update, &mut *transaction).await?;

    transaction.commit().await?;

    let actual_last_update = get_feed_last_update(region, &pool).await?;

    assert_eq!(expected_last_update, actual_last_update.unwrap());
    Ok(())
}
