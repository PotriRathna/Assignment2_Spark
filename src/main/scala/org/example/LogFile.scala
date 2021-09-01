package org.example
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions.regexp_extract

object Logfile extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)
  implicit val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("LogFile")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  // 1.	Write a function to load it in an RDD.
  val logDf = spark.read.textFile("src/main/resources/ghtorrent-logs.txt")
  val parsed_log: DataFrame = Service1.parsefile(logDf)

  println(s"Number of lines in the RDD : ${Service1.count_lines(parsed_log)}")
  println(s"Number of Warning message : ${Service1.count_WARN(parsed_log)}")
  println(s"Number of repositories processed : ${Service1.count_repos(parsed_log)}")

}






