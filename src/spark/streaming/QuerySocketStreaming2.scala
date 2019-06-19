package spark.streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

object QuerySocketStreaming2 extends App {
  
  val sparkSession = SparkSession.builder().getOrCreate()
  
  import sparkSession.implicits._
  
  val dataSchema = new StructType()
  .add("key", StringType)
  .add("speed", DoubleType)
  .add("latitude", DoubleType)
  .add("longitude", DoubleType)
  
  println("Ready for receive data streams...")
  
  val dataStreamDF = sparkSession.readStream
  .schema(dataSchema)
  .option("sep", ",")
  .csv("/home/neumann/data/streaming")
  
  val countInputStrings = dataStreamDF.groupBy($"key")
  .agg(
      sum($"speed").as("TotalSpeed"),
      avg($"speed").as("AVGSpeed"),
      max($"latitude").as("MaxLatitude"),
      max($"longitude").as("MaxLongitude")
  )
  
  val resultWritter = countInputStrings.writeStream.format("console").outputMode(OutputMode.Complete()).start()
  
  resultWritter.awaitTermination()
}