package org.example
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.regexp_extract

object Logfile {

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("LogFile")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
   // 1.	Write a function to load it in an RDD.
    import spark.implicits._
    val logDf = spark.read.textFile("src/main/resources/ghtorrent-logs.txt")

    val parse_logdf = logDf.select(regexp_extract($"value", """^([^(\s|,)]+)""", 1).alias("host"),
      regexp_extract($"value", """([^\s]+)\+00:00""", 1).as("timestamp"),
      regexp_extract($"value", """ghtorrent-([^\s]+)""", 1).cast("int").as("downloader_id"),
      regexp_extract($"value", """([^\s]+).rb: """, 1).alias("retrieval_stage"),
      regexp_extract($"value", """([^\s]+)$""",1).as("rest"))
    parse_logdf.show(4, false)

    // 2.	How many lines does the RDD contain?

    println(s"Number of lines in the RDD : ${parse_logdf.count}")

 //   3.	Count the number of WARNing messages
    parse_logdf.groupBy("host").count().filter($"host"==="WARN").show()
   // println(s"Number of Warning message : ${parse_logdf.filter($"host"==="WARN").groupBy("host").count().show()}")

   // 4.	How many repositories where processed in total? Use the api_client lines only

   val number_repos = parse_logdf.filter($"retrieval_stage" === "api_client").distinct().count()
  println(s"Number of repositories processed : $number_repos")

  }
}
