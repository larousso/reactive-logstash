package com.adelegue.reactive.logstash.output

import java.text.SimpleDateFormat
import java.util.Date
import java.util.concurrent.atomic.AtomicReference

import akka.stream.scaladsl.Sink
import com.adelegue.reactive.logstash.output.elasticsearch.Client
import org.reactivestreams.{Subscription, Subscriber}
import play.api.libs.json.{JsNull, JsValue, Json}

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.io.Source
import scala.util.Try

/**
 *
 * Created by adelegue on 22/11/14.
 */


object ElasticSearchOutput {
  def apply(host: String = "localhost", port: Int = 9200, index: String = "'logstash'-yyyy.MM.dd", typeName: Option[String] = None)(implicit ec: ExecutionContext): Subscriber[JsValue] = {
    val output: ElasticSearchOutput = new ElasticSearchOutput(host, port, index, typeName)(ec)
    Await.result(output.setUp(), 1 second)
    output
  }
}


class ElasticSearchOutput(host: String, port: Int, index: String, typeName: Option[String] = None)(implicit ec: ExecutionContext) extends OutputSubscriber[JsValue] {

  val client = Client(host, port)

  def setUp() = {
    client.createTemplate("logstash", jsonTemplate)
  }

  def jsonTemplate: JsValue = {
    Try(Source
      .fromInputStream(getClass.getResourceAsStream("/com/adelegue/reactive/logstash/output/elasticsearch/elasticsearch-template.json")
    )).map(rs => Json.parse(rs.mkString)).getOrElse(JsNull)
  }

  val indexPattern = new SimpleDateFormat(index)

  override def output(message: JsValue): Future[Unit] = {
    val indexName = indexPattern.format(new Date())
    client.createIndex(indexName, typeName.getOrElse("logs"))().flatMap{esIndex =>
      val value: Future[JsValue] = esIndex.saveAsJsValue(message)
      value.onFailure{
        case e  => println(e)
      }
      value.onSuccess{
        case resp => println(resp)
      }
      value.map(resp => ())
    }
  }


}

