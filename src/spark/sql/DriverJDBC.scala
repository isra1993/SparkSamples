package spark.sql

import org.apache.spark.sql.SparkSession
import java.util.Properties
import org.apache.spark.sql.SaveMode

object DriverJDBC extends App {
  
  val JDBC_URL = "jdbc:postgresql://localhost:5432/cursos"
  val ORC_FILE_PATH = "/home/neumann/data/orc"
    
  val sparkSession = SparkSession.builder().getOrCreate()
  
  val jdbcConfiguration = new Properties()
  jdbcConfiguration.put("user", "cursos")
  jdbcConfiguration.put("password", "cursos")
  
  // Number of partitions for loading process
  //jdbcConfiguration.put("numPartitions", "4")
  // When include property "numPartitions" it is necessary to specify witch field ID should be partitioned and bounds
  //jdbcConfiguration.put("partitionColumn", "customer_id")
  //jdbcConfiguration.put("lowerBound", "1")
  //jdbcConfiguration.put("upperBound", "10000000000000000")
  
  def importConcreteTableDBData(table : String) = {
    
    val customerDF = sparkSession.read.jdbc(JDBC_URL, table, jdbcConfiguration)
    
    customerDF.printSchema()
    
    customerDF.show(20, false)
  }
  
  //importConcreteTableDBData("customer")
  
  def importQueryDBData(query : String) = {
   
    val queryDF = sparkSession.read.jdbc(JDBC_URL, query, jdbcConfiguration)
    
    queryDF.printSchema()
    
    queryDF.show(20, false)
  }
  
  var query = 
    """
      (SELECT customer_id, first_name, last_name, address, postal_code
      FROM Customer
      JOIN Store
      ON Customer.store_id = Store.store_id
      JOIN Address
      ON Customer.address_id = Address.address_id) AS Query
    """
  
  //importQueryDBData(query)
  
  val flightsDF = sparkSession.read.orc(ORC_FILE_PATH)
  
  flightsDF.createOrReplaceTempView("FLIGHTS")
  
  sparkSession.sql(
      """
        SELECT Origin, COUNT(*) AS FlightsNumber
        FROM FLIGHTS
        WHERE Month = 2
        GROUP BY Origin
        ORDER BY Origin
      """
  )
  .write
  .mode(SaveMode.Overwrite)
  .jdbc(JDBC_URL, "Report_test", jdbcConfiguration)
  
  sparkSession.stop()
}