package spark.streaming

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.storage.StorageLevel

object SocketStreaming extends App {
  
  val conf = new SparkConf()
  val sparkStreamingContext = new StreamingContext(conf, Seconds(2))
  
  val socketStreamReader = sparkStreamingContext.socketTextStream("localhost", 5550, StorageLevel.MEMORY_ONLY)
  
  val dataStream = socketStreamReader.map(
      (string) => {
        val stringValues = string.split(",")
        (stringValues(0), (stringValues(1).toDouble, stringValues(2).toDouble, stringValues(3).toDouble))
      }
  )
  .reduceByKeyAndWindow(
      (a : (Double, Double, Double), b: (Double, Double, Double)) => {
        (a._1 + b._1, a._2 + b._2, a._3 + b._3)
      },
      Seconds(10),
      Seconds(20)
  )
  
  // Only for testing
  dataStream.print(100)
  
  // Start data stream processing
  sparkStreamingContext.start()
  
  // Lock thread until data stream processing has finished
  sparkStreamingContext.awaitTermination()
  
  // It is necessary to capture termination signal 
}