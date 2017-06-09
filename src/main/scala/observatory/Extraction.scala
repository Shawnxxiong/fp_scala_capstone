package observatory

import java.time.LocalDate
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import java.nio.file.Paths
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD
import scala.util.matching.Regex

/**
  * 1st milestone: data extraction
  */
object Extraction {
  
  val spark = SparkSession.builder().appName("capstone").config("spark.master", "local").getOrCreate()
  
  import spark.implicits._

  /**
    * @param year             Year number
    * @param stationsFile     Path of the stations resource file to use (e.g. "/stations.csv")
    * @param temperaturesFile Path of the temperatures resource file to use (e.g. "/1975.csv")
    * @return A sequence containing triplets (date, location, temperature)
    */
  def locateTemperatures(year: Int, stationsFile: String, temperaturesFile: String): Iterable[(LocalDate, Location, Double)] = {
    val stationsDF = spark.sparkContext.textFile(stationsFile).map { line =>  
      line.split(",").toList match {
        case List(stn, wban, latitude, longetude) if(latitude != null && longetude != null) => 
          Station(stn = Option(stn), wban = Option(wban), latitude = Option(latitude.toDouble), longetude = Option(longetude.toDouble))
      }
    }.toDF().persist()
    
    val temperatureDF = spark.sparkContext.textFile(temperaturesFile).map { line =>  
      line.split(",").toList match {
        case List(stn, wban, month, day, temperatureInFehranheit) => 
          Temperature(stn = Option(stn), wban = Option(wban), month = month.toInt, day = day.toInt, temperature = toCelcius(Option(temperatureInFehranheit).getOrElse("9999.9").toDouble))
      }
    }.toDF().persist()
    
    val result = stationsDF.join(temperatureDF, stationsDF("stn") <=> temperatureDF("stn") && stationsDF("wban") <=> temperatureDF("wban"), "inner").collectAsList()
    
    Seq(null, null, null)
  }

  /**
    * @param records A sequence containing triplets (date, location, temperature)
    * @return A sequence containing, for each location, the average temperature over the year.
    */
  def locationYearlyAverageRecords(records: Iterable[(LocalDate, Location, Double)]): Iterable[(Location, Double)] = {
    ???
  }
  
  def toCelcius(fahrenheit: Double): Double = (fahrenheit - 32.0) * 5.0 / 9.0
  
  
  def extractYearFromFileName(filename: String): Option[Integer] = {
    val pattern = """/([0-9]{4}).csv""".r
    pattern.findFirstMatchIn(filename) match {
      case Some(m) => Some(m.group(1).toInt)
      case _ => None
    }
  }

}

case class Station(stn:Option[String], wban:Option[String], latitude:Option[Double], longetude:Option[Double])
case class Temperature(stn:Option[String], wban:Option[String], month:Integer, day:Integer, temperature:Double)
case class stationTemperatureRow()