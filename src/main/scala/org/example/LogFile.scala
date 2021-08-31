package org.example
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions.regexp_extract

object Logfile extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)
  val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("LogFile")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  // 1.	Write a function to load it in an RDD.
  val logDf = spark.read.textFile("src/main/resources/ghtorrent-logs.txt")
  val parsed_log: DataFrame = parsefile(logDf,spark)

  println(s"Number of lines in the RDD : ${count_lines(parsed_log)}")
  println(s"Number of Warning message : ${count_WARN(parsed_log,spark)}")
  println(s"Number of repositories processed : ${count_repos(parsed_log,spark)}")

  def parsefile(file: Dataset[String],spark:SparkSession):DataFrame = {

    import spark.implicits._
    val parse_logdf = file.select(regexp_extract($"value", """^([^(\s|,)]+)""", 1).alias("host"),
      regexp_extract($"value", """([^\s]+)\+00:00""", 1).as("timestamp"),
      regexp_extract($"value", """ghtorrent-([^\s]+)""", 1).cast("int").as("downloader_id"),
      regexp_extract($"value", """([^\s]+).rb: """, 1).alias("retrieval_stage"),
      regexp_extract($"value", """([^\s]+)$""", 1).as("rest"))
    // parse_logdf.show(4, false)
    parse_logdf
  }
  // 2.	How many lines does the RDD contain?
  def count_lines(lines: DataFrame):Long = lines.count()

  //   3.	Count the number of WARNing messages
  def count_WARN(lines: DataFrame,spark:SparkSession): Long = {
    import spark.implicits._
    lines.filter($"host"==="WARN").distinct().count()
  }

  // 4.	How many repositories where processed in total? Use the api_client lines only
  def count_repos(lines: DataFrame,spark:SparkSession): Long= {
    import spark.implicits._
    lines.filter($"retrieval_stage" === "api_client").distinct().count()
  }
}






