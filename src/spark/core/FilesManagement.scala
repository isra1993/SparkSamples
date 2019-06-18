package spark.core

import org.apache.spark.SparkContext

object FilesManagement extends App {
  val sparkContext = new SparkContext()
  
  var DATA_FILE_PATH = "file:///home/neumann/Downloads/data/1987.csv.bz2"
  
  // Custom data structure
  case class SimpleFlyingData(
      date : String,
      source : String,
      destiny : String,
      distance : Double,
      departureDelay : Double,
      arrivalDelay : Double
  )

  def generateObjectFiles() = {
    
    val fileRDD = sparkContext.textFile(DATA_FILE_PATH)
    
    def generateSimpleFlyingData(string : String) : (String, SimpleFlyingData) = {
      val valuesString = string.split(",")
      
      def readDoubleValue(value : String) : Double = {
        try {
          value.toDouble
        } catch {
          case t: Throwable => 0.0
        }
      }
      
      (
          valuesString(16), 
          SimpleFlyingData(
              "%04d%02d%02d".format(valuesString(0).toInt, valuesString(1).toInt, valuesString(2).toInt),
              valuesString(16),
              valuesString(17),
              readDoubleValue(valuesString(18)),
              readDoubleValue(valuesString(15)),
              readDoubleValue(valuesString(14))
          )
      )
    }
    
    val flightsRDD = fileRDD.filter(!_.startsWith("Year")).map(generateSimpleFlyingData)
    
    flightsRDD.saveAsObjectFile("/home/neumann/data/object")
  }
  
  // generateObjectFiles()
  
  val flightsRDD = sparkContext.objectFile[(String, SimpleFlyingData)]("/home/neumann/data/object")
  
  val flightUpTo600Miles = flightsRDD.filter(_._2.distance >= 600).count()
  
  printf("Flights up to 600 miles = %d%n", flightUpTo600Miles)
  
  sparkContext.stop()
}