package org.example
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest._
import org.scalatest.funsuite.AnyFunSuite

class TestLogfile extends AnyFunSuite with BeforeAndAfterEach{
  Logger.getLogger("org").setLevel(Level.ERROR)
  implicit val sparksession = SparkSession.builder().master("local[*]").appName("LogFile").getOrCreate()
  import sparksession.implicits._

  val logDf =List(("INFO, 2017-03-22T20:11:49+00:00, ghtorrent-31 -- ghtorrent.rb: Added pullreq_commit 244eeac28bf419642d5d5c3b388bd2999c8c72e6 to tgstation/tgstation -> 25341"),
    ("DEBUG, 2017-03-23T11:15:14+00:00, ghtorrent-30 -- retriever.rb: Commit mzvast/FlappyFrog -> 80bf5c5fde7be6274a2721422f4d9a773583f73c exists"),
    ("DEBUG, 2017-03-22T20:15:48+00:00, ghtorrent-35 -- ghtorrent.rb: Parent af8451e16e077f7c6cae3f98bf43bffaca562f88 for commit 2ef393531a3cfbecc69f17d2cedcc95662fae1e6 exists"),
    ("DEBUG, 2017-03-24T12:29:50+00:00, ghtorrent-49 -- ghtorrent.rb: Parent cf060bf3b789ac6391b2f7c1cdc34191c2bc773d for commit 8c924c1115e1abddcaddc27c6e7fd5806583ea90 exists"),
    ("WARN, 2017-03-23T20:04:28+00:00, ghtorrent-13 -- api_client.rb: Failed request. URL: https://api.github.com/repos/greatfakeman/Tabchi/commits?sha=Tabchi&per_page=100, Status code: 404, Status: Not Found, Access: ac6168f8776, IP: 0.0.0.0, Remaining: 3031"),
    ("DEBUG, 2017-03-23T09:00:44+00:00, ghtorrent-8 -- retriever.rb: Commit iamtheanon/d3 -> a7caf9375fe14d7235562af541fe9decf499bbfb exists"),
    ("DEBUG, 2017-03-24T10:52:47+00:00, ghtorrent-50 -- ghtorrent.rb: Repo alyuev/urantia-study-edition exists")).toDF
  import sparksession.implicits._
  val createDS = logDf.as[String]
  val parse_file = Service1.parsefile(createDS)

  assert(Service1.count_lines(parse_file)===7)
  assert(Service1.count_WARN(parse_file)===1)
  assert(Service1.count_repos(parse_file)===1)
}
