package demo

import java.util.Date
import akka.stream.scaladsl.{SubscriberSink, Source}
import com.adelegue.reactive.logstash.Import._
import com.adelegue.reactive.logstash.filter.MultilineFilter
import com.adelegue.reactive.logstash.input.publisher.RedisPublisher
import com.adelegue.reactive.logstash.output.ElasticSearchOutput
import play.api.libs.json.{JsObject, Json}

object Indexer {

  def main(args: Array[String]) {

    Source(RedisPublisher())
      //Maj de la date
      //.transform("multiline", MultilineFilter())
      .map(json => json.as[JsObject] ++ Json.obj("@timestamp" -> new Date().getTime))
      .map{l => println(s"GING TO PUBLISH TO ES : $l") ; l}
      .runWith(SubscriberSink(ElasticSearchOutput("localhost", 9200)))

  }

}
