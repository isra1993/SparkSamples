package spark.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

object DataLakeCreation extends App {
    
  var DATA_FILE_PATH = "/home/neumann/Downloads/data/1988.csv.bz2"
  val CSV_DEFAULT_SEPARATOR = ","
  val CSV_DEFAULT_ENCODING = "UTF-8"
  val ORC_FILE_PATH = "/home/neumann/data/orc"
  
  val sparkSession = SparkSession.builder().getOrCreate()
  
  
  def importFlightData() = {
    val flightsDF = sparkSession.read.format("csv")
    .option("sep", CSV_DEFAULT_SEPARATOR)
    .option("encoding", CSV_DEFAULT_ENCODING)
    .option("header", true)
    .option("inferSchema", true)
    .option("mode", "PERMISSIVE")
    .load(DATA_FILE_PATH)
    
    flightsDF.write
    .partitionBy("Month")
    .orc(ORC_FILE_PATH)
  }
  
  //importFlightData()
  
  val flightsDF = sparkSession.read.orc(ORC_FILE_PATH)
  
  flightsDF.createOrReplaceTempView("FLIGHTS")
  
  sparkSession.sql(
      """
        SELECT Origin, COUNT(*) AS FlightsNumber
        FROM FLIGHTS
        WHERE Month = 2
        GROUP BY Origin
        ORDER BY Origin
      """
  ).show(40, false)
  
  sparkSession.stop()
}