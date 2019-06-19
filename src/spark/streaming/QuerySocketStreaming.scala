package spark.streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode

object QuerySocketStreaming extends App {
  
  val sparkSession = SparkSession.builder().getOrCreate()
  
  import sparkSession.implicits._
  
  println("Ready for receive data streams...")
  
  val dataStreamDF = sparkSession.readStream.format("socket")
  .option("host", "localhost")
  .option("port", 5550)
  .load()
  
  val inputStingsDF = dataStreamDF.as[String].flatMap((string) => string.split(" "))
  
  val countInputStrings = inputStingsDF.groupBy("value").count()
  
  val resultWritter = countInputStrings.writeStream.format("console").outputMode(OutputMode.Complete()).start()
  
  resultWritter.awaitTermination()
}