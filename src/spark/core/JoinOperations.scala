package spark.core

import org.apache.spark.SparkContext

object JoinOperations extends App {
  
  val sparkContext = new SparkContext()
  
  val films = List(
      raw"tt0000001;short;Carmencita;Carmencita;0;1894;\N      1       Documentary,Short",
      raw"tt0000002;short;Le clown et ses chiens;Le clown et ses chiens;0;1892;    \N    5Animation,Short",
      raw"tt0000003;short;Pauvre Pierrot;Pauvre Pierrot;0;1892;    \N      4       Animation,Comedy,Romance",
      raw"tt0000004;short;Pauvre Pierrot;Pauvre Pierrot;0;1892;    \N      4       Animation,Comedy,Romance"      
  )

  val authors = List(
      raw"tt0000001;nm0005690;\N",  
      raw"tt0000002;nm0721526;\N",
      raw"tt0000003;nm0000003;\N"      
  )
  
  val personalData = List(
      raw"nm0005690;Fred Astaire;1899;1987    soundtrack,actor,miscellaneous  tt0043044,tt0053137,tt0072308,tt0050419",
      raw"nm0721526;Lauren Bacall;1924;2014    actress,soundtrack      tt0038355,tt0117057,tt0037382,tt0071877",
      raw"nm0000003;Brigitte Bardot;1934;\N      actress,soundtrack,producer     tt0054452,tt0049189,tt0059956,tt0057345"      
  )
  
  val filmsRDD = sparkContext.parallelize(films).map(
      (string) => {
        val valuesString = string.split(";")
        (valuesString(0), (valuesString(2), valuesString(5)))
      }
  )
  
  println("#" * 60)
  println("Films:")
  println(filmsRDD.collect().mkString("\n"))
  
  val authorsRDD = sparkContext.parallelize(authors).map(
      (string) => {
        val valuesString = string.split(";")
        (valuesString(0), valuesString(1))
      }
  )
  
  println("#" * 60)
  println("Authors:")
  println(authorsRDD.collect().mkString("\n"))
  
  val filmsAuthorsRDD = filmsRDD.join(authorsRDD)
  
  println("#" * 60)
  println("Films joined with Authors:")
  println(filmsAuthorsRDD.collect().mkString("\n"))
  
  val personalDataRDD = sparkContext.parallelize(personalData).map(
      (string) => {
        val valuesString = string.split(";")
        (valuesString(0), (valuesString(1), valuesString(2)))
      }
  )
  
  println("#" * 60)
  println("Personal data:")
  println(personalDataRDD.collect().mkString("\n"))
  
  val filmsAuthorsAndPersonalDataRDD = filmsAuthorsRDD.map(
      (filmsAuthorsData) => {
        val (titleID, ((title, year), authorID)) = filmsAuthorsData
        (authorID, (titleID, title, year))
      }
  )
  .join(personalDataRDD)
  .map(
      (data) => {
        val (authorID, ((titleID, title, year), (authorName, birthDate))) = data
        s"$titleID;$title;$year;$authorID;$authorName;$birthDate;"
      }
  )
  
  println("#" * 60)
  println("Authors and Films joined with Personal data:")
  println(filmsAuthorsAndPersonalDataRDD.collect().mkString("\n"))
  
  sparkContext.stop()
}