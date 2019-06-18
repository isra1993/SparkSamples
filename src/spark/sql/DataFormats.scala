package spark.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

object DataFormats extends App {
    
  val STUDENTS_FILE_PATH = "/home/neumann/Downloads/data/students.json"
  val COMPANIES_FILE_PATH = "/home/neumann/Downloads/data/companies.json"
  val PARQUET_FILE_PATH = "/home/neumann/data/parquet"
    
  val sparkSession = SparkSession.builder().getOrCreate()
  
  val studentsDF = sparkSession.read.json(STUDENTS_FILE_PATH)
  
  //println("#" * 60)
  //studentsDF.printSchema()
  
  studentsDF.createOrReplaceTempView("STUDENTS")
  
  sparkSession.sql(
      """
        SELECT * FROM STUDENTS
      """
  ).show(20, false)
  
  val companiesDF = sparkSession.read.json(COMPANIES_FILE_PATH)
  
  //println("#" * 60)
  //companiesDF.printSchema()
  
  companiesDF.createOrReplaceTempView("COMPANIES")
  
  val queryDF = sparkSession.sql(
      """
        SELECT Name, Number_of_employees, Phone_number, Founded_year
        FROM COMPANIES
      """
  )
  
  queryDF.coalesce(1).write.mode(SaveMode.Overwrite).parquet(PARQUET_FILE_PATH)
  
  sparkSession.stop()
}