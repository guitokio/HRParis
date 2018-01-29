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

import java.text.SimpleDateFormat;
import java.sql.Timestamp;
import java.util.TimeZone


/**
 *  Runnable object that creates table with actual timetable
 */
object LoadActualTimetable extends App{

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
                      option("header", "false").
                      schema(StructType(Array(StructField("row", StringType	, nullable = false)))).
                      load("/root/paris/actual/")
    

    TimeZone.setDefault(TimeZone.getTimeZone("UTC"))
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm")  
    val rdd_parsed=df.rdd.map(x=> parseRawFile.parse(x.getString(0))).
                filter(x=> ((x._1!=null) && (x._2!=null) && (x._3!=null) && (x._4!=null))).
                map(x=>(parseFields.parse(new Timestamp(dateFormat.parse(x._1+" "+x._2).getTime()),x._3,x._4, Array(x._1,x._2,x._3,x._4).mkString("-")))).
                flatMap(x => x)
                
    //rdd_parsed.repartition(1).saveAsTextFile("file:///c:/Users/415308/workspace/paris/salida.txt")
                
    import spark.implicits._
    rdd_parsed.toDF().schema.foreach(x=>log.file("[INFO] schema: "+x))
    rdd_parsed.take(10).foreach(x=>log.file("[INFO] row: "+nice(x)) )
   
    val df_def=rdd_parsed.toDF().
      withColumn("line",Udf.udftrimInteger(col("line"))).
      withColumn("stop",Udf.udftrimInteger(col("stop"))).
      withColumn("direction",Udf.udfTrimString(col("direction"))).
      withColumn("arrival_time",unix_timestamp($"arrival_time","yyyyMMddHHmm").cast("timestamp")).
      withColumn("delay",Udf.udftrimInteger(col("delay"))).
      withColumn("message",Udf.udfTrimString(col("message"))).
      drop("all")
    
    df_def.schema.foreach(x=>log.file("[INFO] extended schema: "+x))
    df_def.rdd.take(10).foreach(x=>log.file("[INFO] extended row: "+x) )
    
    df_def.write.format("parquet").mode("overwrite").saveAsTable("paris.timetable_actual") 
    log.file ("[INFO] Written to table: paris.timetable_actual")

    log.file ("[INFO] Exiting scala :"+app)
    spark.stop()   
  }
}