package org.example
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest._
import org.scalatest.funsuite.AnyFunSuite

class TestLogfile extends AnyFunSuite with BeforeAndAfterEach{

  Logger.getLogger("org").setLevel(Level.ERROR)
  val sparksession = SparkSession.builder().master("local[*]").appName("LogFile").getOrCreate()
  val logDf = sparksession.read.textFile("src/main/resources/ghtorrent-logs.txt")
  import sparksession.implicits._
  val parse_file: DataFrame = Logfile.parsefile(logDf,sparksession)

  assert(Logfile.count_lines(parse_file)===parse_file.count())
  assert(Logfile.count_WARN(parse_file,sparksession)===parse_file.filter($"host"==="WARN").distinct().count())
  assert(Logfile.count_repos(parse_file,sparksession)===parse_file.filter($"retrieval_stage" === "api_client").distinct().count())
}
