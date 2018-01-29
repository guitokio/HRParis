package com.alstom.paris.analytics

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


/**
 *  Runnable object that creates a grid with actual timetable delays per line and direction
 */
object DelayGrid extends App{
  
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
    
    /*
     138_284     Porte de Clichy-Metro  
     138_225_280 Victor Hugo-Jean Jaures |  
     138_252_263 Curton |  
     138_244_270 General Leclerc-Victor Hugo |  
     138_226_287 Republique-Francois Mitterrand |  
     138_302_305 Villeneuve |  
     138_265_276 Cimetiere |  
     138_274_300 Quai de Clichy |  
     138_231_238 Pierre Boudou |  
     138_258_297 Gresillons-Laurent Cely |  
     138_224_272 Paul Vaillant Couturier |  
     138_233_254 Parc des Sevines |  
     138_235_261 Moulin de Cage | 
     138_278     Rond-Point Pierre Timbaud-J.Larose |  
     138_218_321 Rond Point Pierre Timbaud |  
     138_293_295 Les Barbanniers |  
     138_220_268 Route du Port |  
     138_237_259 Champs Fourgons |  
     138_273_291 Dequevauvilliers |  
     138_266_298 Pont d'Epinay |  
     138_230_239 Gilbert Bonnemaison |  
     138_329_332 Joffre Cinema |  
     138_330_331 Cygne d'Enghien-Joffre |  
     138_228_303 Cygne d'Enghien |  
     138_246_257 Gros Buisson |  
     138_222_309 Gare de Saint Gratien |  
     138_281_307 Berthie Albrecht |  
     138_248_255 Forum |  
     138_219_275 Cite Jean Moulin |  
     138_236_241 Stade Michel Hidalgo |  
     138_251_277 Rue de Soisy |  
     138_216     Gare d'Ermont Eaubonne-RER 
 		*/
    
    import spark.implicits._
    val stops=Array(284,225,252,244,226,302,265,274,231,258,224,233,235,278,218,293,220,237,273,266,230,329,330,228,246,222,281,248,219,236,251,216)
    var df = spark.sql("select stamp,stop,delay from paris.timetable_actual where line=138 and next=false and arrival_time is not null and direction='A' and stop in ("+stops.mkString(",")+")")
    
    stops.map{x=> 
      df=df.withColumn(x.toString(), when($"stop" === x, $"delay").otherwise(null))
      }
    
    val coalesce_exprs=df.columns.filter(x=>x!="stamp" && x!="stop" && x!="delay").map(x=> collect_set(x).alias(x))
                       
    var df_grid=df.drop("stop","delay").groupBy("stamp").agg(coalesce_exprs.head, coalesce_exprs.tail: _*)
    
    df_grid.columns.filter(_!="stamp").map{x=>
       df_grid=df_grid.withColumn(x,col(x).getItem(0))
    }
    
    df_grid.orderBy("stamp").coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").mode("overwrite").save("/root/paris/reports/grid")
  }              
                      

}