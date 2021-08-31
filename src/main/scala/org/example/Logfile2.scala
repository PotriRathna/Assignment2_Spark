package org.example
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import java.text.SimpleDateFormat
import java.util.Date


object Logfile2 extends App {
  case class LogLine(debug_level: String, timestamp: Date, download_id: Integer,
                     retrieval_stage: String, rest: String)
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder().master("local").appName("LogFile").getOrCreate()
    val dateFormat = "yyyy-MM-dd:HH:mm:ss"
    val regex = """([^\s]+), ([^\s]+)\+00:00, ghtorrent-([^\s]+) -- ([^\s]+).rb: (.*$)""".r

    val logrdd = spark.sparkContext.
      textFile("src/main/resources/ghtorrent-logs.txt").
      flatMap ( x => x match {
        case regex(debug_level, dateTime, downloadId, retrievalStage, rest) =>
          val date = new SimpleDateFormat(dateFormat)
          new Some(LogLine(debug_level, date.parse(dateTime.replace("T", ":")), downloadId.toInt, retrievalStage, rest))
        case _ => None
      })
       httprequest(logrdd)
  // 5.	Which client did most HTTP requests?
   def httprequest(rdd : RDD[LogLine]):Any = {
     val http_request = rdd.filter(_.retrieval_stage == "api_client").
       keyBy(_.download_id).
       mapValues(l => 1).
       reduceByKey((a, b) => a + b).
       sortBy(x => x._2, false).
       take(8).toList
     println(s" The client with most HTTP Request :  $http_request ")
   }
    // 6.	Which client did most FAILED HTTP requests? Use group_by to provide an answer.
   val failed_http= logrdd.filter(_.retrieval_stage == "api_client").
      filter(_.rest.startsWith("Failed")).
      keyBy(_.download_id).
      mapValues(l => 1).
      reduceByKey((a,b) => a + b).
      sortBy(x => x._2, false).
      take(8).toList
     println(s"The clients with most failed HTTP request : $failed_http")

    // 7.	What is the most active hour of day?
   val active_hour= logrdd.keyBy(_.timestamp.getHours).
      mapValues(l => 1).
      reduceByKey((a,b) => a + b).
      sortBy(x => x._2, false).
      take(8).toList
     println(s"The most active hours : $active_hour")

  // 8.	What is the most active repository (hint: use messages from the ghtorrent.rb layer only)?
  val repos = logrdd.filter(_.retrieval_stage == "api_client").
    map(_.rest.split("/").slice(4,6).mkString("/").takeWhile(_ != '?'))
  val active_repos = repos.
    filter(_.nonEmpty).
    map(x => (x, 1)).
    reduceByKey((a,b) => a + b).
    sortBy(x => x._2, false).take(5).toList
  println(s"The most active repositories : $active_repos")
}
