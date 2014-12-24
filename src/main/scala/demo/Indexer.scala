package demo

import java.text.SimpleDateFormat
import java.util.Date
import akka.stream.scaladsl.{SubscriberSink, Source}
import com.adelegue.reactive.logstash.Import._
import com.adelegue.reactive.logstash.filter.MultilineFilter
import com.adelegue.reactive.logstash.input.publisher.RedisPublisher
import com.adelegue.reactive.logstash.output.ElasticSearchOutput
import play.api.libs.json.{JsObject, Json}

object Indexer {

  val DATE_ISO = s"(\\d{4}\\-\\d{2}\\-\\d{2} \\d{2}:\\d{2}:\\d{2},\\d{3})"
  val LOG_LEVEL = "(DEBUG|INFO|WARNING|ERROR)"
  val WORD = "(\\w+)"
  val NUMBER = "(\\-?\\d+\\.\\d+)"
  val INTEGER = "(\\d+)"
  val DATE = "(\\d{2}\\-\\d{2}\\-\\d{4})"
  val NOTSPACE = "(\\S+)"
  val DATA = "(.*)"
  val pattern = s"^(?s)$DATE_ISO \\- \\[$LOG_LEVEL\\] \\- from $WORD in $NOTSPACE message numéro $INTEGER current time $DATE, bois : $NOTSPACE, coord : \\[$NUMBER, $NUMBER\\]$DATA$$".r


  def main(args: Array[String]) {

    Source(RedisPublisher())
      //Maj de la date
      .transform("multiline", MultilineFilter(pattern = s"^$DATE_ISO .*$$", negate = true))
      .map{ json =>
        (json \ "message").as[String] match {
          case pattern(timestamp, loglevel, appender, thread, messageNum, messageDate, wood, lat, long, message) =>
            val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS")

            json.as[JsObject] ++
              Json.obj(
                "@timestamp" -> format.parse(timestamp),
                "loglevel" -> loglevel,
                "appender" -> appender,
                "thread" -> thread,
                "messageNum" -> messageNum.toInt,
                "messageDate" -> messageDate,
                "wood" -> wood,
                "coord" -> Json.obj("lat" -> lat, "lng" -> long)
              )
          case _ => json
        }
      }
      .map{l => println(s"GOING TO PUBLISH TO ES : $l") ; l}
      .runWith(SubscriberSink(ElasticSearchOutput("localhost", 9200)))

  }

}
