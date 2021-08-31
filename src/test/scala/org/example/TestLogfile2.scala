package org.example
import org.apache.spark.sql.SparkSession
import org.example.Logfile2.LogLine
import org.scalatest._
import org.scalatest.funsuite.AnyFunSuite

import java.io.Serializable
import java.text.SimpleDateFormat
class TestLogfile2 extends AnyFunSuite with BeforeAndAfterEach{

  val spark = SparkSession.builder().master("local").appName("LogFile").getOrCreate()
  val dateFormat = "yyyy-MM-dd:HH:mm:ss"
  val regex = """([^\s]+), ([^\s]+)\+00:00, ghtorrent-([^\s]+) -- ([^\s]+).rb: (.*$)""".r

  val logrdd = spark.sparkContext.
    textFile("src/main/resources/ghtorrent-logs.txt").
    flatMap ( y => y match {
      case regex(debug_level, dateTime, downloadId, retrievalStage, rest) =>
        val date = new SimpleDateFormat(dateFormat)
        new Some(LogLine(debug_level, date.parse(dateTime.replace("T", ":")), downloadId.toInt, retrievalStage, rest))
      case _ => None
    })
  assert(Logfile2.httprequest(logrdd)=== logrdd.filter(_.retrieval_stage == "api_client").
    keyBy(_.download_id).
    mapValues(l => 1).
    reduceByKey((a, b) => a + b).
    sortBy(x => x._2, false).
    take(8).toList)

}
