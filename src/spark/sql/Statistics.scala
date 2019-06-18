package spark.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Statistics extends App {
    
  var DATA_FILE_PATH = "/home/neumann/Downloads/data/1987.csv"
  val CSV_DEFAULT_SEPARATOR = ","
  val CSV_DEFAULT_ENCODING = "UTF-8"
  
  val sparkSession = SparkSession.builder().getOrCreate()
  
  import sparkSession.implicits._
  
  val flightsDF = sparkSession.read.format("csv")
  .option("sep", CSV_DEFAULT_SEPARATOR)
  .option("encoding", CSV_DEFAULT_ENCODING)
  .option("header", true)
  .option("inferSchema", true)
  .option("mode", "PERMISSIVE")
  .load(DATA_FILE_PATH)
  
  // Basic statistics for all columns (Min, Max, STD, Count)
  // val basicStatisticsDF = flightsDF.describe()
  
  val basicStatisticsDF = flightsDF.describe("CRSDepTime", "DepDelay")
  
  basicStatisticsDF.show
  
//  val statisticsDF1 = flightsDF.select("Origin", "Distance", "ArrDelay", "DepDelay", "TaxiIn", "TaxiOut")
//  .withColumn("Distance", 'Distance.cast("Int"))
//  .groupBy("Origin")
//  .avg("Distance", "ArrDelay", "DepDelay", "TaxiIn", "TaxiOut")
  
  val correclationDepArrDF = flightsDF.select("Origin", "ArrDelay", "DepDelay")
  .groupBy("Origin")
  .agg(
      corr($"DepDelay", $"ArrDelay").alias("CorrelationDepArr"),
      covar_pop($"DepDelay", $"ArrDelay").as("CovarDepArr"),
      avg($"DepDelay").as("AvgDepDelay"), 
      max($"DepDelay").as("MaxDepDelay"), 
      min($"DepDelay").as("MinDepDelay"),
      stddev($"DepDelay").as("STDDepDelay"),
      variance($"DepDelay").as("VarDepDelay")
   )
  
  correclationDepArrDF.show()
  
  sparkSession.stop()
}