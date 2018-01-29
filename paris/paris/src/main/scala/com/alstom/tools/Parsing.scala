package com.alstom.tools

import com.alstom.paris.classes.{TimetableActual}
import java.sql.Timestamp
import java.util.TimeZone
import java.text.SimpleDateFormat

/**
 *  Parser object of raw actual timetable files 
 */
object parseRawFile extends Serializable {
  
  val rowPattern = """^(\d*-\d*-\d*);(\d*:\d*);([\d_]*);(.*)""".r
  def parse(s:String):(String,String,String,String)  ={
    s match {
      case rowPattern(date,time,lineYstops,message) => (date,time,lineYstops,message)
      case _ => (null,null,null,null)
    }
  }
}

/**
 *  Parser object of every single message of the raw actual tiemtable files
 */
object parseFields extends Serializable  {   
  val optionsTime ="""-?\d+|A l'arret|A l'approche"""
  val optionsMessage = """BUS SUIVANT DEVIE|SERVICE|PERTURBATIONS|NON COMMENCE|PREMIER PASSAGE|DERNIER PASSAGE|SERVICE TERMINE|DEVIATION|PAS DE SERVICE|ARRET NON DESSERVI|ARRET REPORTE|\.+|INFO INDISPO[\s]*\.+"""
  val baseTime ="""["]*null[\s]*null[\s]*([A|R])[\s]*(\d+)[\s]*("""+optionsTime+""")[\smn]*"""
  val baseMessage ="""["]*null[\s]*null[\s]*([A|R])[\s]*null[\s]*("""+optionsMessage+""")"""
 
  def parse(time: Timestamp, stops:String, messages: String, text: String):Array[TimetableActual]={
    val nmb=stops.trim.split('_').size
    val items=stops.trim.split('_')
    if ((nmb == 1) &  (items(0)=="")) return Array(TimetableActual(time,null,null,null,null,null,null,false,null))
    else if ((nmb == 1) &  (items(0)!="")) return Array(TimetableActual(time,items(0),null,null,null,null,null,false,null))
    else {
      // there are at least one line number and one stop ID
      val line= items(0)
      val stops_array = items.takeRight(nmb-1)
      val messages_array=messages.trim.split('|').grouped(2).toArray
      val iterator= stops_array zip messages_array
      iterator.map(tuple=>tuple match{
          case(stopId,mssgs) => {
            Array(1,2) zip mssgs map{ x=> x match{
              case (1,msg) => msg.trim match {
                case baseTime.r(direction,arrival_time,"A l'arret") => TimetableActual(time,line,stopId,direction,arrival_time,"0",null,false,text)
                case baseTime.r(direction,arrival_time,"A l'approche") => TimetableActual(time,line,stopId,direction,arrival_time,"0",null,false,text)
                case baseTime.r(direction,arrival_time,delay) => TimetableActual(time,line,stopId,direction,arrival_time,delay,null,false,text)
                case baseMessage.r(direction,message) => TimetableActual(time,line,stopId,direction,null,null,message,false,text) 
                case _ => TimetableActual(time,"error","error","error","error","error",msg,false,text)                
              } //case1
              case (2,msg) => msg.trim match {
                case baseTime.r(direction,arrival_time,"A l'arret") => TimetableActual(time,line,stopId,direction,arrival_time,"0",null,true,text)
                case baseTime.r(direction,arrival_time,"A l'approche") => TimetableActual(time,line,stopId,direction,arrival_time,"0",null,true,text)
                case baseTime.r(direction,arrival_time,delay) => TimetableActual(time,line,stopId,direction,arrival_time,delay,null,true,text)
                case baseMessage.r(direction,message) => TimetableActual(time,line,stopId,direction,null,null,message,true,text) 
                case _ => TimetableActual(time,"error","error","error","error","error",msg,true,text)  
              } //case2
            }} //Array(1,2) zip mssgs 
          } //case(stopId,mssgs)
        } //match tuple
      ).flatten //map iterator
    }
  } //def
}

/* QUERIES FOR TESTING
val s="null null A 201712220829 2 mn | null null A 201712220836 8 mn | null null R 201712220832 5 mn | null null R 201712220835 8 mn | "
val s=" null null A 201712220006 6 mn | null null A 201712220006 6 mn | null null R 201712220000 A l'arret | null null R 201712220023 23 mn | "
val s=" null null A 201712220006 A l'arret | null null A 201712220006 6 mn | null null R 201712220000 5 mn | null null R 201712220023 23 mn | "
val s=" null null A 201712220006 A l'arret | null null A 201712220006 6 mn | null null R 201712220000 A l'approche | null null R 201712220023 23 mn | "
val s="null null A 201712220833 6 mn | null null A 201712220837 10 mn | "
val s="null null A 201712220833 A l'arret | null null A 201712220837 A l'approche | "
val s="null null A 201712220833 8 mn | null null A 201712220837 A l'approche | "
val s="null null R 201712220024 7 mn | null null R 201712220050 33 mn | "
val s="null null R 201712220024 A l'arret | null null R 201712220050 A l'approche | "
val s="null null R 201712220024 7 mn | null null R 201712220050 A l'arret | "
val s="null null A null SERVICE | null null A null NON COMMENCE | null null R null SERVICE | null null R null NON COMMENCE | "
val s="null null A null NON COMMENCE  | null null A null SERVICE | null null R null NON COMMENCE | null null R null SERVICE | "
val s="null null A null SERVICE TERMINE | null null A null .................. | null null R null SERVICE TERMINE | null null R null .................. | "
val s="null null A null INFO INDISPO .... | null null A null INFO INDISPO .... | null null R null SERVICE | null null R null NON COMMENCE | "
val s="null null A null INFO INDISPO .... | null null A null INFO INDISPO .... | null null R null INFO INDISPO .... | null null R null INFO INDISPO .... | "
val s="null null A null SERVICE | null null A null NON COMMENCE | "
val s="null null A null NON COMMENCE  | null null A null SERVICE | "
val s="null null A null SERVICE TERMINE | null null A null .................. | "
val s="null null A null INFO INDISPO .... | null null A null INFO INDISPO .... | "
val s="null null R null SERVICE | null null R null NON COMMENCE | "
val s="null null R null NON COMMENCE  | null null R null SERVICE | "
val s="null null R null SERVICE TERMINE | null null R null .................. | "
val s="null null R null INFO INDISPO .... | null null R null INFO INDISPO .... | "
val s="null null A null DEVIATION | null null A null ARRET NON DESSERVI | null null R null DEVIATION | null null R null ARRET NON DESSERVI | "
val s="null null A null DEVIATION | null null A null ARRET NON DESSERVI | "
val s=" null null R null DEVIATION | null null R null ARRET NON DESSERVI | "
val s="null null A 201712220008 8 mn | null null A 201712220034 34 mn | null null R 201712220034 34 mn | null null R null ARRET NON DESSERVI | "
val s="null null A 201712220008 8 mn | null null A 201712220034 34 mn | null null R null ARRET NON DESSERVI | null null R 201712220034 34 mn | "
val s="null null A 201712220008 8 mn | null null A 201712220034 34 mn | null null R null ARRET NON DESSERVI | null null R null DEVIATION | "
val s="null null A 201712220008 8 mn | null null A null ARRET NON DESSERVI | null null R 201712220034 34 mn | null null R 201712220034 34 mn | "
val s="null null A 201712220008 8 mn | null null A null ARRET NON DESSERVI | null null R 201712220034 34 mn | null null R null DEVIATION | "
val s="null null A 201712220008 8 mn | null null A null ARRET NON DESSERVI | null null R null DEVIATION |null null R 201712220034 34 mn |  "
val s="null null A 201712220008 8 mn | null null A null ARRET NON DESSERVI | null null R null DEVIATION |null null R null INFO INDISPO .... |  "
val s="null null A null ARRET NON DESSERVI | null null A 201712220034 34 mn  | null null R 201712220034 34 mn | null null R 201712220034 34 mn | "
val s="null null A null ARRET NON DESSERVI | null null A 201712220034 34 mn  | null null R 201712220034 34 mn | null null R null INFO INDISPO .... |  "
val s="null null A null ARRET NON DESSERVI | null null A 201712220034 34 mn  | null null R null INFO INDISPO ....  | null null R null ARRET NON DESSERVI | "
val s="null null A null ARRET NON DESSERVI | null null A null DEVIATION  | null null R 201712220034 34 mn  | null null R null ARRET NON DESSERVI | "
val s="null null A null ARRET NON DESSERVI | null null A null DEVIATION  | null null R null INFO INDISPO ....   | null null R 201712220034 34 mn   | "
val s="null null A 201712220008 8 mn | null null A null ARRET NON DESSERVI | "
val s="null null A null ARRET NON DESSERVI | null null A 201712220008 8 mn | "
val s="null null R 201712220008 8 mn | null null R null ARRET NON DESSERVI | "
val s="null null R null ARRET NON DESSERVI | null null R 201712220008 8 mn | "
*/