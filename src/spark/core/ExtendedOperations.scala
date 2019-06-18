package spark.core

import org.apache.spark.SparkContext

object ExtendedOperations extends App {
  
  val sparkContext = new SparkContext()
  
  var DATA_FILE_PATH = "file:///home/neumann/Downloads/data/1987.csv.bz2"
  val fileRDD = sparkContext.textFile(DATA_FILE_PATH)
  
  val monthsFlightRDD = fileRDD.keyBy(_.split(",")(1))
  
  for((monthString, line) <- monthsFlightRDD.take(4)) {
    printf("Month: %s -> Line: [%s]%n", monthString, line)
  }
  
  fileRDD.filter(
      (string) => {
        try {
          string.split(",")(12).toDouble
          true
        } catch {
          case t: Throwable => false
        }
      }
  )
  .map(
      (string) => {
        var stringValues = string.split(",")
        (stringValues(16), stringValues(12).toDouble)
      }
  )
  .aggregateByKey((0.0, 0))(
      // seqOp -> Reduce operation for each partition without shuffling
      (tuple, flightTime) => {
        (tuple._1 + flightTime, tuple._2 + 1)
      }, 
      // combOp -> Reduce operation for each partial result
      (prev, next) => {
        (prev._1 + next._1, prev._2 + next._2)
      }
  )
  .map(
      data => {
        val (flight, (flightTime, flightCount)) = data 
        s"$flight;$flightTime;$flightCount;"
      }
  )
  .coalesce(1)
  .saveAsTextFile("/tmp/reportFlightTime")
  
  sparkContext.stop()
}