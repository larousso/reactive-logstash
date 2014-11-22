package com.adelegue.reactive.logstash.output

import com.adelegue.reactive.logstash.output.elasticsearch.HttpClient
import org.scalatest.{FlatSpec, Matchers}
import play.api.libs.json.Json
import play.api.libs.ws.WS
import scala.concurrent.ExecutionContext.Implicits.global
//import play.api.Play.current
import scala.concurrent.duration.DurationDouble
import scala.concurrent.{Await, Future}

/**
 * Created by adelegue on 22/11/14.
 */
class ElasticSearchOutputSpec extends FlatSpec with Matchers {

  "ES output" should "return template" in {
    val esOutput: ElasticSearchOutput = new ElasticSearchOutput("localhost", 9200, "'logstash'-yyyy.MM.dd")
    println(esOutput.jsonTemplate)
  }

  "ES output" should "create template" in {
    val esOutput: ElasticSearchOutput = new ElasticSearchOutput("localhost", 9200, "'logstash'-yyyy.MM.dd")
    Await.result(esOutput.setUp(), 1 second)
    Await.result(HttpClient().url("http://localhost:9200/_template/logstash").get(), 1 second).status shouldBe 200
  }

  "ES output" should "index document" in {
    val output = ElasticSearchOutput("localhost", 9200)
    val apply: Future[Unit] = output.apply(Json.obj("test" -> "test message"))

    Await.result(apply, 1 second)

    val json = Await.result(HttpClient().url("http://localhost:9200/logstash-*/_search?q=*").get().map(r => Json.parse(r.body)), 1 second)

    (json \ "hits" \ "total").as[Int] should be >= 1

  }

}
