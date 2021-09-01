package org.example
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.scalatest._
import org.scalatest.funsuite.AnyFunSuite

class TestService2_Logfile2 extends AnyFunSuite with BeforeAndAfterEach{

  Logger.getLogger("org").setLevel(Level.ERROR)
  implicit val spark = SparkSession.builder().master("local[*]").appName("LogFile").getOrCreate()
  val logrdd=Service2_Logfile2.parsefile()

  assert(Service2_Logfile2.httprequest(logrdd).toList=== List((13,3983), (21,2988), (40,908), (20,834), (42,796), (2,783), (47,745), (4,744)))
  assert(Service2_Logfile2.failedrequest(logrdd).toList ===List((13,2321), (21,43), (40,27), (22,17), (1,14),(42,13)))
  assert(Service2_Logfile2.active(logrdd).toList===List((10,78000), (9,69950), (11,68685), (12,38054), (13,11657), (20,7989), (14,2407), (16,390)))
  assert(Service2_Logfile2.activerepos(logrdd).toList===List(("greatfakeman/Tabchi",2318), ("mithro/chromium-infra",115),
    ("shuhongwu/hockeyapp",74), ("obophenotype/human-phenotype-ontology",73), ("ssbattousai/Cuda36",39)))

  spark.stop()
}
