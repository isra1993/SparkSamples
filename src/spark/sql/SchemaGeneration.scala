package spark.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

object SchemaGeneration extends App {
    
  var DATA_FILE_PATH = "/home/neumann/Downloads/data/1987.csv"
  val CSV_DEFAULT_SEPARATOR = ","
  val CSV_DEFAULT_ENCODING = "UTF-8"
  
  val sparkSession = SparkSession.builder().getOrCreate()
  
  val aSchema = new StructType()
  .add("OriginAirport", StringType, false)
  .add("DestinationAirport", StringType, false)
  .add("DistanceInMilles", IntegerType, false)
  
  val flightsRDD = sparkSession.sparkContext.textFile(DATA_FILE_PATH)
  .map(
      (string) => {
        val valuesString = string.split(",")
        
        def readInteger(value : String) : Integer = {
          try {
            value.toInt
          } catch {
            case t: Throwable => -1
          }
        }
        Row(
            valuesString(16), 
            valuesString(17), 
            readInteger(valuesString(18))
        )
      }
  )
  
  val flightsDF = sparkSession.createDataFrame(flightsRDD, aSchema)
  
  flightsDF.show()
  
  flightsDF.createOrReplaceTempView("FLIGHTS")
   
   val reportDF = sparkSession.sql(
       """
         SELECT OriginAirport, SUM(DistanceInMilles) AS TotalDistance 
         FROM FLIGHTS 
         GROUP BY OriginAirport 
         ORDER BY OriginAirport
         """
    )
     
   reportDF.show()
   
  sparkSession.stop()
}