package org.example
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.scalatest._
import org.scalatest.funsuite.AnyFunSuite

class TestLogfile2 extends AnyFunSuite with BeforeAndAfterEach{

  Logger.getLogger("org").setLevel(Level.ERROR)
  val spark = SparkSession.builder().master("local[*]").appName("LogFile").getOrCreate()
  val logrdd=Logfile2.parsefile(spark)

  assert(Logfile2.httprequest(logrdd)=== logrdd.filter(_.retrieval_stage == "api_client").
    keyBy(_.download_id).
    mapValues(l => 1).reduceByKey((a, b) => a + b).
    sortBy(x => x._2, false).take(8))

  assert(Logfile2.failedrequest(logrdd) === logrdd.filter(_.retrieval_stage == "api_client").
    filter(_.rest.startsWith("Failed")).keyBy(_.download_id).
    mapValues(l => 1).reduceByKey((a, b) => a + b).
    sortBy(x => x._2,false).take(8))

  assert(Logfile2.active(logrdd)===logrdd.keyBy(_.timestamp.getHours).
    mapValues(l => 1).reduceByKey((a, b) => a + b).
    sortBy(x => x._2,false).take(8))

  val repository = logrdd.filter(_.retrieval_stage == "api_client").
    map(_.rest.split("/").slice(4, 6).mkString("/").takeWhile(_ != '?'))
  assert(Logfile2.activerepos(logrdd)===repository.filter(_.nonEmpty).
    map(x => (x, 1)).reduceByKey((a, b) => a + b).
    sortBy(x => x._2,false).take(5))

  spark.stop()
}
