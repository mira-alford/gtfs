-- GTFS-realtime schema (PostgreSQL)
-- Based on gtfs-realtime.proto reference

-- ============================================================================
-- DROP TABLES (dependency order)
-- ============================================================================

DROP TABLE IF EXISTS alert_url_text CASCADE;
DROP TABLE IF EXISTS alert_description_text CASCADE;
DROP TABLE IF EXISTS alert_header_text CASCADE;
DROP TABLE IF EXISTS translated_string CASCADE;
DROP TABLE IF EXISTS alert_informed_entity CASCADE;
DROP TABLE IF EXISTS alert_time_range CASCADE;
DROP TABLE IF EXISTS alert CASCADE;
DROP TABLE IF EXISTS vehicle_position CASCADE;
DROP TABLE IF EXISTS stop_time_update CASCADE;
DROP TABLE IF EXISTS trip_update CASCADE;
DROP TABLE IF EXISTS feed_entity CASCADE;
DROP TABLE IF EXISTS feed_message CASCADE;

-- ============================================================================
-- FEED MESSAGE
-- ============================================================================

CREATE TABLE IF NOT EXISTS feed_message
(
  id                      bigserial PRIMARY KEY,
  header_timestamp        bigint NOT NULL,
  header_version          text   NOT NULL,
  header_incrementality   smallint NOT NULL CHECK (header_incrementality IN (0,1))
  -- 0 = FULL_DATASET, 1 = DIFFERENTIAL
);

-- ============================================================================
-- FEED ENTITY
-- ============================================================================

CREATE TABLE IF NOT EXISTS feed_entity
(
  id                      text PRIMARY KEY,
  feed_message_id         bigint NOT NULL REFERENCES feed_message(id) ON DELETE CASCADE,
  is_deleted              boolean NOT NULL DEFAULT false,
  entity_type             smallint NOT NULL CHECK (entity_type IN (0,1,2))
  -- 0 = TRIP_UPDATE, 1 = VEHICLE_POSITION, 2 = ALERT
);

-- ============================================================================
-- TRIP UPDATE
-- ============================================================================

CREATE TABLE IF NOT EXISTS trip_update
(
  entity_id               text PRIMARY KEY REFERENCES feed_entity(id) ON DELETE CASCADE,

  trip_id                 text NULL,
  route_id                text NULL,
  start_time              text NULL,
  start_date              text NULL,

  schedule_relationship   smallint NULL CHECK (schedule_relationship IN (0,1,2,3,4,5)),
  -- 0=SCHEDULED,1=ADDED,2=UNSCHEDULED,3=CANCELED,4=REPLACEMENT,5=DUPLICATED

  vehicle_id              text NULL,
  vehicle_label           text NULL,
  vehicle_license_plate   text NULL,

  timestamp               bigint NULL,
  delay                   integer NULL
);

CREATE TABLE IF NOT EXISTS stop_time_update
(
  id                      bigserial PRIMARY KEY,
  trip_update_entity_id   text NOT NULL REFERENCES trip_update(entity_id) ON DELETE CASCADE,

  stop_sequence           integer NULL CHECK (stop_sequence >= 0),
  stop_id                 text NULL,

  arrival_time            bigint NULL,
  arrival_delay           integer NULL,
  arrival_uncertainty     integer NULL,

  departure_time          bigint NULL,
  departure_delay         integer NULL,
  departure_uncertainty   integer NULL,

  schedule_relationship   smallint NULL CHECK (schedule_relationship IN (0,1,2,3)),
  -- 0=SCHEDULED,1=SKIPPED,2=NO_DATA,3=UNPLANNED

  stop_headsign           text NULL,
  track                   text NULL
);

-- ============================================================================
-- VEHICLE POSITION
-- ============================================================================

CREATE TABLE IF NOT EXISTS vehicle_position
(
  entity_id               text PRIMARY KEY REFERENCES feed_entity(id) ON DELETE CASCADE,

  trip_id                 text NULL,
  route_id                text NULL,
  start_time              text NULL,
  start_date              text NULL,

  trip_schedule_relationship smallint NULL CHECK (trip_schedule_relationship IN (0,1,2,3,4,5)),

  vehicle_id              text NULL,
  vehicle_label           text NULL,
  vehicle_license_plate   text NULL,

  latitude                double precision NULL,
  longitude               double precision NULL,
  bearing                 real NULL,
  odometer                double precision NULL,
  speed                   real NULL,

  current_stop_sequence   integer NULL CHECK (current_stop_sequence >= 0),
  current_status          smallint NULL CHECK (current_status IN (0,1,2)),
  -- 0=INCOMING_AT,1=STOPPED_AT,2=IN_TRANSIT_TO

  stop_id                 text NULL,
  timestamp               bigint NULL,

  congestion_level        smallint NULL CHECK (congestion_level IN (0,1,2,3,4)),
  -- 0=UNKNOWN,1=RUNNING_SMOOTHLY,2=STOP_AND_GO,3=CONGESTION,4=SEVERE_CONGESTION

  occupancy_status        smallint NULL CHECK (occupancy_status IN (0,1,2,3,4,5,6)),
  -- 0=EMPTY ... 6=NOT_ACCEPTING_PASSENGERS

  occupancy_percentage    integer NULL CHECK (occupancy_percentage >= 0 AND occupancy_percentage <= 100)
);

-- ============================================================================
-- ALERTS
-- ============================================================================

CREATE TABLE IF NOT EXISTS alert
(
  entity_id               text PRIMARY KEY REFERENCES feed_entity(id) ON DELETE CASCADE,

  cause                   smallint NULL CHECK (cause IN (0,1,2,3,4,5,6,7,8,9,10,11)),
  effect                  smallint NULL CHECK (effect IN (0,1,2,3,4,5,6,7,8,9,10)),
  severity_level          smallint NULL CHECK (severity_level IN (0,1,2,3))
);

CREATE TABLE IF NOT EXISTS alert_time_range
(
  id                      bigserial PRIMARY KEY,
  alert_entity_id         text NOT NULL REFERENCES alert(entity_id) ON DELETE CASCADE,
  start                   bigint NULL,
  "end"                   bigint NULL
);

CREATE TABLE IF NOT EXISTS alert_informed_entity
(
  id                      bigserial PRIMARY KEY,
  alert_entity_id         text NOT NULL REFERENCES alert(entity_id) ON DELETE CASCADE,

  agency_id               text NULL,
  route_id                text NULL,
  route_type              integer NULL,
  trip_id                 text NULL,
  stop_id                 text NULL,
  direction_id            boolean NULL
);

-- ============================================================================
-- TRANSLATED STRINGS
-- ============================================================================

CREATE TABLE IF NOT EXISTS translated_string
(
  id                      bigserial PRIMARY KEY,
  language                text NULL,
  text                    text NOT NULL
);

CREATE TABLE IF NOT EXISTS alert_header_text
(
  alert_entity_id         text NOT NULL REFERENCES alert(entity_id) ON DELETE CASCADE,
  translated_string_id    bigint NOT NULL REFERENCES translated_string(id) ON DELETE CASCADE,
  PRIMARY KEY (alert_entity_id, translated_string_id)
);

CREATE TABLE IF NOT EXISTS alert_description_text
(
  alert_entity_id         text NOT NULL REFERENCES alert(entity_id) ON DELETE CASCADE,
  translated_string_id    bigint NOT NULL REFERENCES translated_string(id) ON DELETE CASCADE,
  PRIMARY KEY (alert_entity_id, translated_string_id)
);

CREATE TABLE IF NOT EXISTS alert_url_text
(
  alert_entity_id         text NOT NULL REFERENCES alert(entity_id) ON DELETE CASCADE,
  translated_string_id    bigint NOT NULL REFERENCES translated_string(id) ON DELETE CASCADE,
  PRIMARY KEY (alert_entity_id, translated_string_id)
);
