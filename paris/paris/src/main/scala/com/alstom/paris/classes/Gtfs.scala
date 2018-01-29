package com.alstom.paris.classes

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{to_date, to_timestamp}
import scala.collection.mutable.ArrayBuffer
import scala.collection.immutable.ListMap

case class GtfsAgency(
  agency_id: String,
  agency_name: String,
  agency_url: String,
  agency_timezone: String,
  agency_lang: String,
  agency_phone: String,
  agency_fare_url: String
)

case class GtfsCalendar(
  service_id: String,
  monday: Int,
  tuesday: Int,
  wednesday: Int,
  thursday: Int,
  friday: Int,
  saturday: Int,
  sunday: Int,
  start_date: java.sql.Date,
  end_date: java.sql.Date
)

case class GtfsCalendarDates(
  service_id: String,
  date: java.sql.Date,
  exception_type: Int
)

case class GtfsFareAttributes(
  fare_id: String,
  price: Double,
  currency_type: String,
  payment_method: Int,
  transfers: Int,
  transfer_duration: Int
)

case class GtfsFareRules(
  fare_id: String,
  route_id: String,
  origin_id: String,
  destination_id: String,
  contains_id: String
)

case class GtfsFeedInfo(
  feed_publisher_name: String,
  feed_publisher_url: String,
  feed_lang: String,
  feed_start_date: java.sql.Date,
  feed_end_date: java.sql.Date,
  feed_version: String
)

case class GtfsFrequencies(
  trip_id: String,
  start_time: java.sql.Timestamp,
  end_time: java.sql.Timestamp,
  headway_secs: Int,
  exact_times: Int
)

case class GtfsRoutes(
  route_id: String,
  agency_id: String,
  route_short_name: String,
  route_long_name: String,
  route_desc: String,
  route_type: Int,
  route_url: String,
  route_color: String,
  route_text_color: String
)

case class GtfsShapes(
  shape_id: String,
  shape_pt_lat: Double,
  shape_pt_lon: Double,
  shape_pt_sequence: Long,
  shape_dist_traveled: Double
)

case class GtfsStopTimes(
  trip_id: String,
  arrival_time: java.sql.Timestamp,
  departure_time: java.sql.Timestamp,
  stop_id: String,
  stop_sequence: Int,
  stop_headsign: String,
  pickup_type: Int,
  drop_off_type: Int,
  shape_dist_traveled: Double
)

case class GtfsStops(
  stop_id: String,
  stop_code: String,
  stop_name: String,
  stop_desc: String,
  stop_lat: Double,
  stop_lon: Double,
  zone_id: String ,
  stop_url: String,
  location_type: Int,
  parent_station: String,
  stop_timezone: String,
  wheelchair_boarding: Int
)

case class GtfsTransfers(
  from_stop_id: String,
  to_stop_id: String,
  transfer_type: Int,
  min_transfer_time: Int
)

case class GtfsTrips(
  route_id: String,
  service_id: String,
  trip_id: String,
  trip_headsign: String,
  trip_short_name: String,
  direction_id: Int,
  block_id: String,
  shape_id: String,
  wheelchair_accessible: Int
)
