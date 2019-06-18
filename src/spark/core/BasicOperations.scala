package spark.core

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object BasicOperations extends App {
  
  // Spark runtime configuration
  var sparkConfig = new SparkConf()
  
  // Application name
  sparkConfig.setAppName("Basic Operations v1.0")
  
  // Set master for spark submit execution
  // sparkConfig.setMaster("local[*]")
  
  // Spark runtime
  val sparkContext = new SparkContext(sparkConfig)
  
  // Text file reading
  // sparkContext.textFile(path, minPartitions)
  
  // Whole text files reading
  // sparkContext.wholeTextFiles(path, minPartitions)
  
  // Binary files reading
  // sparkContext.binaryFiles(path, minPartitions)
  
  // Sequence files reading (Hadoop)
  // sparkContext.sequenceFile(path, keyClass, valueClass)
  
  // Hadoop recognized files reading
  // sparkContext.hadoopFile(path)
  
  var DATA_FILE_PATH = "file:///home/neumann/Downloads/data/1987.csv"
  
  val fileRDD = sparkContext.textFile(DATA_FILE_PATH)
  
  // Actions -> Access RDD content
  val firstElement = fileRDD.first()
  
  println("#" * 60)
  printf("First element:%n%s%n", firstElement)
  
  val tenElements = fileRDD.take(10)
  println("#" * 60)
  printf("Ten elements:%n%s%n", tenElements.mkString("\n"))
  
  val numberOfFlys = fileRDD.count()
  println("#" * 60)
  printf("Number of flys: %d%n", numberOfFlys)
  
  // Transformations -> Generate RDD from other RDD
  
  // String Mapper  
  def getDistance(string : String) : Double = {
    val stringValues = string.split(",")
    try {
      stringValues(18).toDouble
    } catch {
      case t: Throwable => 0.0
    }
  }
  
  // Using custom map function
  val distancesRDD1 = fileRDD.map(getDistance)
  // Using map with compact format
  val distancesRDD2 = fileRDD.map(_(18).toDouble)
  val totalDistance1 = distancesRDD1.sum()
  val totalDistance2 = distancesRDD1.sum()
  println("#" * 60)
  printf("Total distance 1: %f miles%nTotal distance 2: %f miles%n", totalDistance1, totalDistance2)
  
  var numberOfPartitions = distancesRDD1.partitions.length
  println("#" * 60)
  printf("Number of distancesRDD partitions: %d%n", numberOfPartitions)
  
  ///////////////////////////////////////////////////////////////////
  
  // String Mapper  
  def getOriginAndDistance(string : String) : (String, Double) = {
    val stringValues = string.split(",")
    try {
      (stringValues(16), stringValues(18).toDouble)
    } catch {
      case t: Throwable => (stringValues(16), 0.0)
    }
  }
  
  val flysRDD = fileRDD.map(getOriginAndDistance)
  
  val distancesReducction = distancesRDD1.reduce((a, b) => a * 0.2 + b * 0.4)
  
  // Sum of distances with same origin
  val distancesByOrigin = flysRDD.reduceByKey(_ + _).sortByKey(false)
  
  println("#" * 60)
  
  // Be careful with .collect() because requires so much memory depending on collection lenght
  for((origin, distance) <- distancesByOrigin.collect()) {
    printf("Origin: %s, Distance: %8.2f%n", origin, distance)
  }
  
  // Filters
  def originFilter(origin : String)(string : String) : Boolean = {
    string.split(",")(16) == origin
  }
  
  val originSFO = originFilter("SFO") _
  val originLAX = originFilter("LAX") _
  
  def readLongValue(value : String) : Long = {
    try {
      value.toLong
    } catch {
      case t: Throwable => 0
    }
  }
  
  val dataSFORDD = fileRDD.filter(originSFO).map(
      (string) => {
        var stringValues = string.split(",")
        (stringValues(9), 
            (readLongValue(stringValues(15)),
             readLongValue(stringValues(14)),
             readLongValue(stringValues(20)),
             readLongValue(stringValues(19))
             )
         )
      }
  )
  
  val flyesSFO = dataSFORDD.reduceByKey(
      (a, b) => {
        (a._1 + b._1, a._2 + b._2, a._3 + b._3, a._4 + b._4)
      }
  )
  
  flyesSFO.map(
      data => {
        val (flight, (depDelay, arrDelay, taxiOut, taxiIn)) = data 
        s"$flight;$depDelay;$arrDelay;$taxiOut;$taxiIn;"
      }
  ).coalesce(1).saveAsTextFile("/tmp/SFOflyes")
  
  // Disconnect from Spark context
  sparkContext.stop()
}