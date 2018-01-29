package com.alstom.paris

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
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
import com.alstom.tools.Udf

/**
 *  Runnable object that creates table with mapping between BDGEO stops and GTFS
 */
object LoadStopsMapping extends App{
  
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

    val spark = SparkSession.builder().
                  appName(getClass.getName).
                  enableHiveSupport().
                  getOrCreate()
    val fs = FileSystem.get(new Configuration())     
    val df = spark.read.format("com.databricks.spark.csv").
      option("header", "true").
      option("delimiter", ";").
      option("inferSchema", "true").
      load("/root/paris/stops_gtfs_ex_20171130.csv").
      drop("jaccard").
      drop("distance")
      
    log.file("[INFO] Loaded file: "+"/root/paris/stops_gtfs_ex_20171130.csv")
    log.file("[INFO] size:"+df.count())
    df.schema.foreach(x=> log.file("[INFO]     schema: "+x.toString()))
    df.take(10).foreach(x=>log.file("[INFO]     row"+x) ) 
     
    
    import spark.implicits._
    df.write.mode("overwrite").saveAsTable("paris.stops_mapping") 
    log.file ("[INFO] Written to table: paris.stops_mapping")     
    log.file ("[INFO] Exiting scala :"+app)
    spark.stop()   
  }
  
}