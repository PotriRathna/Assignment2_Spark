package org.example
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import java.text.SimpleDateFormat
import java.util.Date

object Service2_Logfile2 {

  case class LogLine(debug_level: String, timestamp: Date, download_id: Integer,
                     retrieval_stage: String, rest: String)

  // 5.	Which client did most HTTP requests?
  def httprequest(rdd : RDD[LogLine]):Array[(Integer,Int)] = {
    val http= rdd.filter(_.retrieval_stage == "api_client").keyBy(_.download_id).
      mapValues(l => 1).reduceByKey((a, b) => a + b).
      sortBy(x => x._2,false).take(8)
    http

  }
  // 6.	Which client did most FAILED HTTP requests? Use group_by to provide an answer.
  def failedrequest(rdd : RDD[LogLine]):Array[(Integer,Int)] = {
    val failed = rdd.filter(_.retrieval_stage == "api_client").
      filter(_.rest.startsWith("Failed")).keyBy(_.download_id).
      mapValues(l => 1).reduceByKey((a, b) => a + b).
      sortBy(x => x._2,false).take(6)
    failed
  }
  // 7.	What is the most active hour of day?
  def active(rdd : RDD[LogLine]):Array[(Int,Int)] = {
    val Active = rdd.keyBy(_.timestamp.getHours).
      mapValues(l => 1).reduceByKey((a, b) => a + b).
      sortBy(x => x._2,false).take(8)
    Active
  }
  // 8.	What is the most active repository (hint: use messages from the ghtorrent.rb layer only)?
  def activerepos(rdd: RDD[LogLine]):Array[(String,Int)]= {
    val repos = rdd.filter(_.retrieval_stage == "api_client").
      map(_.rest.split("/").slice(4, 6).mkString("/").takeWhile(_ != '?'))
    val Active_repos= repos.filter(_.nonEmpty).
      map(x => (x, 1)).reduceByKey((a, b) => a + b).
      sortBy(x => x._2,false).take(5)
    Active_repos
  }
  def parsefile()(implicit spark:SparkSession):RDD[LogLine]={
    val dateFormat = "yyyy-MM-dd:HH:mm:ss"
    val regex = """([^\s]+), ([^\s]+)\+00:00, ghtorrent-([^\s]+) -- ([^\s]+).rb: (.*$)""".r

    val log = spark.sparkContext.
      textFile("src/main/resources/ghtorrent-logs.txt").
      flatMap ( x => x match {
        case regex(debug_level, dateTime, downloadId, retrievalStage, rest) =>
          val date = new SimpleDateFormat(dateFormat)
          new Some(LogLine(debug_level, date.parse(dateTime.replace("T", ":")), downloadId.toInt, retrievalStage, rest))
        case _ => None
      })
    log
  }
}
