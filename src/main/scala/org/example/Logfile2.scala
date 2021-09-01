package org.example
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.example.Logfile.spark

import java.text.SimpleDateFormat
import java.util.Date
object Logfile2 extends App {

    Logger.getLogger("org").setLevel(Level.ERROR)

    implicit val spark = SparkSession.builder().master("local").appName("LogFile").getOrCreate()
  val logrdd = Service2_Logfile2.parsefile()

    println(s"The clients with most HTTP request : ${Service2_Logfile2.httprequest(logrdd).toList}")
    println(s"The clients with most failed HTTP request : ${Service2_Logfile2.failedrequest(logrdd).toList}")
    println(s"The most active hours : ${Service2_Logfile2.active(logrdd).toList}")
    println(s"The most active repositories : ${Service2_Logfile2.activerepos(logrdd).toList}")
}
