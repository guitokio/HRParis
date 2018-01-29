package com.alstom.paris

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

import org.apache.hadoop.conf._
import org.apache.hadoop.fs._

import org.apache.log4j.Logger
import org.apache.log4j.MDC
import org.apache.log4j.Level
import com.alstom.tools.Logs
import com.alstom.tools.Implicits._
import com.alstom.tools.logAssistance.nice
import com.alstom.tools.{parseFields,parseRawFile,Udf}
import com.alstom.tools.GtfsParsing

import java.text.SimpleDateFormat;
import java.sql.Timestamp;
import java.util.TimeZone

import scala.collection.mutable.ArrayBuffer
import scala.collection.immutable.ListMap

/**
 *  Runnable object that creates table with actual timetable
 */
object LoadTheoricalTimetable extends App{

  override def main (args: Array[String])
  {
    if (args.length != 2)
    {
      println ("Spark job arguments must be: <LogFilePath> <DEBUG 0/1>")
      return
    }
          
    val logPath = args(0)
    val logLevel = if (args(1).toInt==1) Level.DEBUG else Level.INFO
    
    val app=getClass.getName
    implicit  @transient lazy val log = Logger.getLogger(app)
    Logs.config_logger(getClass.getName, logPath, logLevel)
    MDC.put("appName", getClass.getName)
    log.file("[INFO] Init LOG: "+getClass.getName)

    implicit val spark = SparkSession.builder().
                  appName(getClass.getName).
                  enableHiveSupport().
                  getOrCreate()
    val fs = FileSystem.get(new Configuration())     
    
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"))
    val gtfs = new GtfsParsing()

    val filesPaths = "/root/paris/timetable/stif_gtfs_20171130"
    val filesPaths_list = fs.listStatus(new Path(filesPaths))
    filesPaths_list.foreach(x=> log.file("[INFO] Files to parse: "+x.getPath.toString))
    
    var staging_path_root = "/root/paris/timetable_parsed/"
    var raw_path = "/root/paris/timetable_parsed_pq/"
    for (file <- filesPaths_list){
      log.file("[INFO] 			Parsing: "+file.getPath.getName)
      val (fileName, fileExtension) = gtfs.getFileNameAndExtFromPath(file.getPath.getName)
      val model_header = gtfs.getHeadersModelStr(fileName)
      val staging_file_path = staging_path_root + "/" + fileName + "." + fileExtension
      val file_header = spark.read.csv(file.getPath.toString).rdd.first().mkString(",")
      val idxField_file_map = (file_header.split(",").indices zip file_header.split(",")).toMap
      val idxField_model_map = (model_header.split(",").indices zip model_header.split(",")).toMap
      val sortedByIdx_model_map = ListMap(idxField_model_map.toSeq.sortWith(_._1 < _._1):_*)
      spark.read.csv(file.getPath.toString).rdd
          .map(_.toSeq)
          .map(x => if (x(0) == model_header.split(",")(0)) model_header.split(",").toList
            else gtfs.orderAndBuild(sortedByIdx_model_map,(file_header.split(",") zip x).toMap))
          .map(x => x.mkString(","))
          .repartition(1)
          .saveAsTextFile(staging_file_path)
      log.file("[INFO] 			Saving to: "+file.getPath.toString)
      spark.read.option("header", "true")
          .csv(staging_file_path).write.mode("overwrite")
          .parquet(raw_path+fileName + ".parquet")
      log.file("[INFO] 			Saving parquet to: "+raw_path+fileName + ".parquet")
    }

    var parquet_file_path = "/root/paris/timetable_parsed_pq/stops.parquet"
    gtfs.create_raw_table(parquet_file_path,gtfs.getFileNameAndExtFromPath(parquet_file_path)._1)    
    parquet_file_path = "/root/paris/timetable_parsed_pq/routes.parquet"
    gtfs.create_raw_table(parquet_file_path,gtfs.getFileNameAndExtFromPath(parquet_file_path)._1)  
    parquet_file_path = "/root/paris/timetable_parsed_pq/stop_times.parquet"
    gtfs.create_raw_table(parquet_file_path,gtfs.getFileNameAndExtFromPath(parquet_file_path)._1)      
    parquet_file_path = "/root/paris/timetable_parsed_pq/trips.parquet"
    gtfs.create_raw_table(parquet_file_path,gtfs.getFileNameAndExtFromPath(parquet_file_path)._1)    
    parquet_file_path = "/root/paris/timetable_parsed_pq/calendar.parquet"
    gtfs.create_raw_table(parquet_file_path,gtfs.getFileNameAndExtFromPath(parquet_file_path)._1)  
    parquet_file_path = "/root/paris/timetable_parsed_pq/calendar_dates.parquet"
    gtfs.create_raw_table(parquet_file_path,gtfs.getFileNameAndExtFromPath(parquet_file_path)._1) 
    parquet_file_path = "/root/paris/timetable_parsed_pq/transfers.parquet"
    gtfs.create_raw_table(parquet_file_path,gtfs.getFileNameAndExtFromPath(parquet_file_path)._1)   
    
    log.file ("[INFO] Exiting scala :"+app)
    spark.stop()   
  }
}