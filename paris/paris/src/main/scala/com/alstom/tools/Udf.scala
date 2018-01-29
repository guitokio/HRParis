package com.alstom.tools

import org.apache.spark.sql.functions._
import net.yageek.lambert._

import java.sql.Timestamp


/**
 *  Object that provides extra udfs for dataframe processing. Basically cleaning of parsed types
 */
object Udf extends Serializable{
  
  val trimString: (String => Option[String]) = (arg: String)  => {if (arg != null) toOptString(arg.trim) else None}
  val udfTrimString = udf(trimString)
  val trimInteger: (String => Option[Int]) = (arg: String)  => {if (arg != null) toOptInt(arg.trim) else None}
  val udftrimInteger = udf(trimInteger)
  val ConvertFromLambert: ((Int, Int) => Option[(Double,Double)]) = (lat : Int, lon : Int) => {if ((lat != null) && (lon != null)) FromLambert(lat,lon) else None} 
  val udfConvertFromLambert = udf(ConvertFromLambert)

 
    
  def toOptInt(s:String): Option[Int] = 
    try { 
      Some(s.toInt) 
    } catch { 
      case e: Throwable => None 
  }
    
  def toOptString(s:String): Option[String] = 
    try { 
      Some(s) 
    } catch { 
      case e: Throwable => None 
  }

  def FromLambert(lat:Int,lon:Int): Option[(Double,Double)] = {
    try { 
      val pt = Lambert.convertToWGS84Deg(lat, lon, LambertZone.LambertIIExtended)
      Some((pt.getY(), pt.getX()))
    } catch { 
      case e: Throwable => None 
    }
  }
  
}