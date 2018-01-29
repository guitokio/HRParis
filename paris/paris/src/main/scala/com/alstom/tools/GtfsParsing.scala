package com.alstom.tools

import scala.collection.mutable.ArrayBuffer
import scala.collection.immutable.ListMap
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{to_date, to_timestamp}

import com.alstom.paris.classes._

class GtfsParsing (implicit spark:org.apache.spark.sql.SparkSession) extends Serializable {
  import spark.implicits._

  //************
  val headersAgency = "agency_id,agency_name,agency_url,agency_timezone,agency_lang,agency_phone,agency_fare_url"
  val headersStops = "stop_id,stop_code,stop_name,stop_desc,stop_lat,stop_lon,zone_id,stop_url,location_type,parent_station,stop_timezone,wheelchair_boarding"
  val headersRoutes = "route_id,agency_id,route_short_name,route_long_name,route_desc,route_type,route_url,route_color,route_text_color"
  val headersTrips = "route_id,service_id,trip_id,trip_headsign,trip_short_name,direction_id,block_id,shape_id,wheelchair_accessible"
  val headersStop_times = "trip_id,arrival_time,departure_time,stop_id,stop_sequence,stop_headsign,pickup_type,drop_off_type,shape_dist_traveled"
  val headersStop_extensions = "stop_id,ZDEr_ID_REF_A"
  val headersCalendar = "service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date"
  val headersCalendar_dates = "service_id,date,exception_type"
  val headersFare_attributes = "fare_id,price,currency_type,payment_method,transfers,transfer_duration"
  val headersFare_rules = "fare_id,route_id,origin_id,destination_id,contains_id"
  val headersShapes = "shape_id,shape_pt_lat,shape_pt_lon,shape_pt_sequence,shape_dist_traveled"
  val headersFrequencies = "trip_id,start_time,end_time,headway_secs,exact_times"
  val headersTransfers = "from_stop_id,to_stop_id,transfer_type,min_transfer_time"
  val headersFeed_info = "feed_publisher_name,feed_publisher_url,feed_lang,feed_start_date,feed_end_date,feed_version"

  // A partir del nombre del fichero (sin extension) obtenemos el header modelo para ese fichero
  def getHeadersModelStr(file: String): String = file match {
    case "agency"          => headersAgency
    case "stops"           => headersStops
    case "routes"          => headersRoutes
    case "trips"           => headersTrips
    case "stop_times"      => headersStop_times
    case "stop_extensions" => headersStop_extensions
    case "calendar"        => headersCalendar
    case "calendar_dates"  => headersCalendar_dates
    case "fare_attributes" => headersFare_attributes
    case "fare_rules"      => headersFare_rules
    case "shapes"          => headersShapes
    case "frequencies"     => headersFrequencies
    case "transfers"       => headersTransfers
    case "feed_info"       => headersFeed_info
  }

  def getFileNameAndExtFromPath(path: String): (String, String) = {
    // Match Strings without / and with .
    val r = "([^/]+\\..*)".r
    // Get file name and file extension in an array of two elements
    val result = r.findFirstIn(path).get.split("\\.")
    (result(0), result(1))
  }

  // This method return a List of elemnts with the same order that the ordModelmap
  def orderAndBuild(ordModelmap: ListMap[Int, String], x: Map[String, Any]): Seq[Any] = {
    var f = ArrayBuffer[Any]()
    for ((k, v) <- ordModelmap) { if (x.keys.toList contains v) f += x(v) else f += None }
    f.toSeq.toList
  }

  def create_raw_table(path_file: String, table_name: String): Unit = table_name match {
    case "shapes" =>
      println("Loading shapes...")
      val gtfsShapesDS = spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .parquet(path_file)
        .withColumn("shape_pt_lat", 'Shape_pt_lat.cast(DoubleType))
        .withColumn("shape_pt_lon", 'Shape_pt_lon.cast(DoubleType))
        .withColumn("shape_pt_sequence", 'Shape_pt_sequence.cast(LongType))
        .withColumn("shape_dist_traveled", 'Shape_dist_traveled.cast(DoubleType))
        .as[GtfsShapes]
      println(gtfsShapesDS.count() + " " + table_name + " loaded.")
      gtfsShapesDS.write.mode("overwrite").saveAsTable("paris.gtfs_" + table_name)

    case "agency" =>
      println("Loading agency...")
      val gtfsAgencyDS = spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .parquet(path_file)
        .as[GtfsAgency];
      println(gtfsAgencyDS.count() + " " + table_name + " loaded.")
      gtfsAgencyDS.write.mode("overwrite").saveAsTable("paris.gtfs_" + table_name)

    case "calendar" =>
      println("Loading calendar...")
      val gtfsCalendarDS = spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .parquet(path_file)
        .withColumn("monday", 'Monday.cast(IntegerType))
        .withColumn("tuesday", 'Tuesday.cast(IntegerType))
        .withColumn("wednesday", 'Wednesday.cast(IntegerType))
        .withColumn("thursday", 'Thursday.cast(IntegerType))
        .withColumn("friday", 'Friday.cast(IntegerType))
        .withColumn("saturday", 'Saturday.cast(IntegerType))
        .withColumn("sunday", 'Sunday.cast(IntegerType))
        .withColumn("start_date", to_date('Start_date, "yyyyMMdd"))
        .withColumn("end_date", to_date('End_date, "yyyyMMdd"))
        .as[GtfsCalendar];
      println(gtfsCalendarDS.count() + " " + table_name + " loaded.")
      gtfsCalendarDS.write.mode("overwrite").saveAsTable("paris.gtfs_" + table_name)

    case "calendar_dates" =>
      println("Loading calendar_dates...")
      val gtfsCalendardatesDS = spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .parquet(path_file)
        .withColumn("date", to_date('Date, "yyyyMMdd"))
        .withColumn("exception_type", 'Exception_type.cast(IntegerType))
        .as[GtfsCalendarDates];
      println(gtfsCalendardatesDS.count() + " " + table_name + " loaded.")
      gtfsCalendardatesDS.write.mode("overwrite").saveAsTable("paris.gtfs_" + table_name)

    case "fare_attributes" =>
      println("Loading fare_attributes...")
      val gtfsFareattributesDS = spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .parquet(path_file)
        .withColumn("price", 'Price.cast(DoubleType))
        .withColumn("payment_method", 'Payment_method.cast(IntegerType))
        .withColumn("transfers", 'Transfers.cast(IntegerType))
        .withColumn("transfer_duration", 'Transfer_duration.cast(IntegerType))
        .as[GtfsFareAttributes];
      println(gtfsFareattributesDS.count() + " " + table_name + " loaded.")
      gtfsFareattributesDS.write.mode("overwrite").saveAsTable("paris.gtfs_" + table_name)

    case "fare_rules" =>
      println("Loading fare_rules...")
      val gtfsFarerulesDS = spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .parquet(path_file)
        .as[GtfsFareRules];
      println(gtfsFarerulesDS.count() + " " + table_name + " loaded.")
      gtfsFarerulesDS.write.mode("overwrite").saveAsTable("paris.gtfs_" + table_name)

    case "feed_info" =>
      println("Loading feed_info...")
      val gtfsFeedinfoDS = spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .parquet(path_file)
        .withColumn("feed_start_date", to_date('Feed_start_date, "yyyyMMdd"))
        .withColumn("feed_end_date", to_date('Feed_end_date, "yyyyMMdd"))
        .as[GtfsFeedInfo];
      println(gtfsFeedinfoDS.count() + " " + table_name + " loaded.")
      gtfsFeedinfoDS.write.mode("overwrite").saveAsTable("paris.gtfs_" + table_name)

    case "frequencies" =>
      println("Loading frequencies...")
      val gtfsFrequenciesDS = spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .parquet(path_file)
        .withColumn("start_time", to_timestamp('Start_time, "HH:mm:ss"))
        .withColumn("end_time", to_timestamp('End_time, "HH:mm:ss"))
        .withColumn("headway_secs", 'Headway_secs.cast(IntegerType))
        .withColumn("exact_times", 'Exact_times.cast(IntegerType))
        .as[GtfsFrequencies];
      println(gtfsFrequenciesDS.count() + " " + table_name + " loaded.")
      gtfsFrequenciesDS.write.mode("overwrite").saveAsTable("paris.gtfs_" + table_name)

    case "routes" =>
      println("Loading routes...")
      val gtfsRoutesDS = spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .parquet(path_file)
        .withColumn("route_type", 'Route_type.cast(IntegerType))
        .as[GtfsRoutes];
      println(gtfsRoutesDS.count() + " " + table_name + " loaded.")
      gtfsRoutesDS.write.mode("overwrite").saveAsTable("paris.gtfs_" + table_name)

    case "stop_times" =>
      println("Loading stop_times...")
      val gtfsStoptimesDS = spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .parquet(path_file)
        .withColumn("arrival_time", to_timestamp('Arrival_time, "HH:mm:ss"))
        .withColumn("departure_time", to_timestamp('Departure_time, "HH:mm:ss"))
        .withColumn("stop_sequence", 'Stop_sequence.cast(IntegerType))
        .withColumn("pickup_type", 'Pickup_type.cast(IntegerType))
        .withColumn("drop_off_type", 'Drop_off_type.cast(IntegerType))
        .withColumn("shape_dist_traveled", 'Shape_dist_traveled.cast(DoubleType))
        .as[GtfsStopTimes];
      println(gtfsStoptimesDS.count() + " " + table_name + " loaded.")
      gtfsStoptimesDS.write.mode("overwrite").saveAsTable("paris.gtfs_" + table_name)

    case "stops" =>
      println("Loading stops...")
      val gtfsStopsDS = spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .parquet(path_file)
        .withColumn("stop_lat", 'Stop_lat.cast(DoubleType))
        .withColumn("stop_lon", 'Stop_lon.cast(DoubleType))
        .withColumn("location_type", 'Location_type.cast(IntegerType))
        .withColumn("wheelchair_boarding", 'Wheelchair_boarding.cast(IntegerType))
        .as[GtfsStops];
      println(gtfsStopsDS.count() + " " + table_name + " loaded.")
      gtfsStopsDS.write.mode("overwrite").saveAsTable("paris.gtfs_" + table_name)

    case "transfers" =>
      println("Loading transfers...")
      val gtfsTransfersDS = spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .parquet(path_file)
        .withColumn("transfer_type", 'Transfer_type.cast(IntegerType))
        .withColumn("min_transfer_time", 'Min_transfer_time.cast(IntegerType))
        .as[GtfsTransfers];
      println(gtfsTransfersDS.count() + " " + table_name + " loaded.")
      gtfsTransfersDS.write.mode("overwrite").saveAsTable("paris.gtfs_" + table_name)

    case "trips" =>
      println("Loading trips...")
      val gtfsTripsDS = spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .parquet(path_file)
        .withColumn("direction_id", 'Direction_id.cast(IntegerType))
        .withColumn("wheelchair_accessible", 'Wheelchair_accessible.cast(IntegerType))
        .as[GtfsTrips];
      println(gtfsTripsDS.count() + " " + table_name + " loaded.")
      gtfsTripsDS.write.mode("overwrite").saveAsTable("paris.gtfs_" + table_name)

    case _ => println("Odd file in folder!")
  } 
  
  
}