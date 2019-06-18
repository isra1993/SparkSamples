package spark.core

import org.apache.spark.SparkContext

object ArrayOperations extends App {
  val sparkContext = new SparkContext()
  
  // var DATA_FILE_PATH = "file:///home/neumann/Downloads/data/1987.csv.bz2"
  
  val data = Array(
    "1987,10,14,3,741,730,912,849,PS,1451,NA,91,79,NA,23,11,SAN,SFO,443,NA,NA,0,NA,0,NA,NA,NA,NA,NA",
    "1987,10,15,4,729,730,903,849,PS,1451,NA,94,79,NA,14,-1,BOS,SFO,444,NA,NA,0,NA,0,NA,NA,NA,NA,NA",
    "1987,10,17,6,741,730,918,849,PS,1451,NA,97,79,NA,29,11,JFK,SFO,445,NA,NA,0,NA,0,NA,NA,NA,NA,NA",
    "1987,10,18,7,729,730,847,849,PS,1451,NA,78,79,NA,-2,-1,LAX,SFO,446,NA,NA,0,NA,0,NA,NA,NA,NA,NA",
    "1987,10,19,1,749,730,922,849,PS,1451,NA,93,79,NA,33,19,JFK,SFO,447,NA,NA,0,NA,0,NA,NA,NA,NA,NA",
    "1987,10,21,3,728,730,848,849,PS,1451,NA,80,79,NA,-1,-2,LAX,SFO,448,NA,NA,0,NA,0,NA,NA,NA,NA,NA"
  ) 
  
  val dataRDD = sparkContext.parallelize(data).map((s) => (s.split(",")(16), s.split(",")(18)))
  
  println("Count of original data: " + dataRDD.count())
  println("Original data:")
  for(v <- dataRDD.collect()) {
    println(v)
  }
  
  val flightsLAXRDD = dataRDD.filter(_._1 == "LAX")
  val flightsJFKRDD = dataRDD.filter(_._1 == "JFK")
  
  val intersectionJFK = dataRDD.intersection(flightsJFKRDD)
  
  println("Count of JFK flights intersection: " + intersectionJFK.count())
  println("Intersection:")
  for(v <- intersectionJFK.collect()) {
    println(v)
  }
  
  val unionLAXCount = flightsJFKRDD.union(flightsLAXRDD)
  
  println("Count of LAX flights union with JFK: " + unionLAXCount.count())
  println("Union:")
  for(v <- unionLAXCount.collect()) {
    println(v)
  }
  
  val substractLAXRDD = dataRDD.subtract(flightsLAXRDD)
  
  println("Count of LAX flights substraction: " + substractLAXRDD.count())
  println("Substraction:")
  for(v <- substractLAXRDD.collect()) {
    println(v)
  }
  
  sparkContext.stop()
}