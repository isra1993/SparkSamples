package spark.core

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object CommonVariables extends App {
  
  var sparkConfig = new SparkConf()
  
  sparkConfig.setAppName("Basic Operations v1.0")
  
  val sparkContext = new SparkContext(sparkConfig)
  
  // It cannot be possible to create variables out of Spark runtime
  val euroToDollarNotValid = 1.18 // NOK
  
  // Shared variable creation
  val euroToDollarValid = sparkContext.broadcast(1.18) // OK
  
  println(s"Conversion EUR -> DOL: ${euroToDollarValid.value}")
  
  ////////////////////////////////////////////////////////////////////////////////
  
  // Accumulators
  val flightsCounter = sparkContext.longAccumulator("flightsCounter")
  // Sets to zero
  flightsCounter.reset()
  // Increment long value
  flightsCounter.add(1)
  
  val totalBalance = sparkContext.doubleAccumulator("totalBalance")
  totalBalance.add(100.22)
  
  println(s"Accumulators:\n\tFlightsCounter = ${flightsCounter.value}\n\tTotalBalance = ${totalBalance.value}")
  
  
  sparkContext.stop()
}