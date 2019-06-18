package spark.core

import org.apache.spark.SparkContext

object FilesManagement2 extends App {
  val sparkContext = new SparkContext()
  
  var DATA_FILE_PATH = "file:///home/neumann/Downloads/data/1987.csv.bz2"
  val OUTPUT_FILE_PATH = "/home/neumann/data/sequence"
  
  def generateSequenceFile() {
  
    val fileRDD = sparkContext.textFile(DATA_FILE_PATH)
    
      def generateFlightsData(l : String) : (String, FlightsData) = {
      
        val valuesString = l.split(',')
        
        def readInteger(cadena : String) : Int = {
          try {
            cadena.toInt
          }
          catch {
            case t: Throwable => -1
          }
        }
            
        (
            valuesString(16),
            FlightsData(
              "%04d-%02d-%02d".format(readInteger(valuesString(0)), readInteger(valuesString(1)), readInteger(valuesString(2))),
              valuesString(4),
              valuesString(5),
              valuesString(6),
              valuesString(7),
              valuesString(8),
              valuesString(9),
              valuesString(10),
              readInteger(valuesString(11)),
              readInteger(valuesString(12)),
              readInteger(valuesString(13)),
              readInteger(valuesString(14)),
              readInteger(valuesString(15)),
              valuesString(16),
              valuesString(17),
              readInteger(valuesString(18)),
              readInteger(valuesString(19)),
              readInteger(valuesString(20)),
              valuesString(21),
              valuesString(22),
              valuesString(23),
              readInteger(valuesString(24)),
              readInteger(valuesString(25)),
              readInteger(valuesString(26)),
              readInteger(valuesString(27)),
              readInteger(valuesString(28))
            )
         )
    }
    
    val flightsRDD = fileRDD.map(generateFlightsData)
    
    flightsRDD.saveAsSequenceFile(OUTPUT_FILE_PATH)
  }
  
  generateSequenceFile()
  
  val dataRDD = sparkContext.sequenceFile[String, FlightsData](OUTPUT_FILE_PATH)
  
  val dataWithIndexRDD = dataRDD.zipWithIndex()
  val dataWithUniqueIdRDD = dataRDD.zipWithUniqueId()
  
  println("#" * 40)
  for(((origin, data), index) <- dataWithIndexRDD.take(10)) {
    println(s"$index: $origin -> ${data.date} - ${data.uniqueCarrier}")
  }
  
  println("#" * 40)
  for(((origin, data), index) <- dataWithUniqueIdRDD.take(10)) {
    println(s"$index: $origin -> ${data.date }- ${data.uniqueCarrier}")
  }
  
  sparkContext.stop()
}