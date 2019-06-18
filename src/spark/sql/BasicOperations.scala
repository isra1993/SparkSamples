package spark.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object BasicOperations extends App {
    
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
  .option("mode", "PERMISSIVE") // By default, loads all data
  //.option("mode", "DROPMALFORMED") // Remove problematic lines
  //.option("mode", "FAILFAST") // Load error when detects a problematic line
  .load(DATA_FILE_PATH)
   
   // Only for development checking
   //flightsDF.printSchema()
  
  def customFunction(data : String) : String = {
    data + " modifed"
  }
  
  val customFunctionUDF = udf(customFunction _)
   
   val testDF1 = flightsDF.select("Origin", "Distance", "Month")
   .withColumn("TestConcat", concat($"Distance", $"Month"))
   .withColumn("TestCustomFunction", customFunctionUDF($"Origin"))
   .withColumn("DistanceInt", 'Distance.cast("Int"))
   .filter("Distance > 800")
   
   testDF1.show(10, false)
   
   val testDF2 = flightsDF.withColumn("Distance", 'Distance.cast("Int"))
   .groupBy("Origin")
   .sum("Distance").withColumnRenamed("sum(Distance)", "TotalDistance")
   
   testDF2.show(10, false)
   
   // Applying SQL queries directly on DataFrame
   flightsDF.createOrReplaceTempView("FLIGHTS")
   
   val reportDF = sparkSession.sql(
       """
         SELECT Origin, SUM(CAST(Distance AS Integer)) AS TotalDistance 
         FROM FLIGHTS 
         GROUP BY Origin 
         ORDER BY Origin
         """
    )
     
   reportDF.show()
   
   flightsDF.createGlobalTempView("FLIGHTSDATA")
   
   sparkSession.newSession().sql(
     """
     SELECT COUNT(*) AS OBS 
     FROM global_temp.FLIGHTSDATA
     """
    ).show()
   
   sparkSession.stop()
}