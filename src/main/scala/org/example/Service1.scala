package org.example

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions.regexp_extract

object Service1  {

   def readDs(path:String)(implicit spark:SparkSession):Dataset[String]={
    spark.read.textFile(path)
  }
  
  def parsefile(file: Dataset[String])(implicit spark:SparkSession):DataFrame = {
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
  def count_WARN(lines: DataFrame)(implicit spark:SparkSession): Long = {
    import spark.implicits._
    lines.filter($"host"==="WARN").distinct().count()
  }

  // 4.	How many repositories where processed in total? Use the api_client lines only
  def count_repos(lines: DataFrame)(implicit spark:SparkSession): Long= {
    import spark.implicits._
    lines.filter($"retrieval_stage" === "api_client").distinct().count()
  }
}
