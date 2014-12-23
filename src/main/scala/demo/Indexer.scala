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
  //val ISO_DATE_REGEX = "^([\\+-]?\\d{4}(?!\\d{2}\\b))((-?)((0[1-9]|1[0-2])(\\3([12]\\d|0[1-9]|3[01]))?|W([0-4]\\d|5[0-2])(-?[1-7])?|(00[1-9]|0[1-9]\\d|[12]\\d{2}|3([0-5]\\d|6[1-6])))([T\\s]((([01]\\d|2[0-3])((:?)[0-5]\\d)?|24\\:?00)([\\.,]\\d+(?!:))?)?(\\17[0-5]\\d([\\.,]\\d+)?)?([zZ]|([\\+-])([01]\\d|2[0-3]):?([0-5]\\d)?)?)?)?"
  val LOG_LEVEL = "(DEBUG|INFO|WARNING|ERROR)"
  val WORD = "(\\w+)"
  val NUMBER = "(\\d+\\.\\d+)"
  val INTEGER = "(\\d+)"
  val DATE = "(\\d{2}\\-\\d{2}\\-\\d{4})"
  val NOTSPACE = "(\\S+)"
  val DATA = "(.*)"
  val pattern = s"^$DATE_ISO \\- \\[$LOG_LEVEL\\] \\- from $WORD in $NOTSPACE message numéro $INTEGER current time $DATE, bois : $NOTSPACE, coord : \\[$NUMBER, $NUMBER\\] $DATA$$".r


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
                "coord" -> Json.obj("lat" -> lat, "lng" -> long),
                "message" -> message
              )
          case _ => json
        }
      }
      .map{l => println(s"GOING TO PUBLISH TO ES : $l") ; l}
      .runWith(SubscriberSink(ElasticSearchOutput("localhost", 9200)))

  }

}
