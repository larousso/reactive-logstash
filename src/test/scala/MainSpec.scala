import java.io.File
import java.util
import java.util.Date
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}

import akka.actor.ActorSystem
import akka.stream
import akka.stream.FlowMaterializer
import akka.stream.scaladsl.{ForeachSink, Source, SubscriberSink}
import com.adelegue.reactive.logstash.filter.MultilineFilter
import com.adelegue.reactive.logstash.input.publisher.{FilePublisher, RedisPublisher}
import com.adelegue.reactive.logstash.output.{ElasticSearchOutput, RedisOutput}
import com.google.common.io.Files
import org.apache.commons.io.FileUtils
import org.reactivestreams.{Subscriber, Subscription}
import org.scalatest._
import play.api.libs.json.{JsObject, JsValue, Json}
import scredis.Redis

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.immutable.Seq
import scala.concurrent.{Future, Promise}

class MainSpec extends FlatSpec with Matchers {

  "test conflate" should "" in {

    implicit val system = ActorSystem("SysConflate")
    implicit val materializer = FlowMaterializer()
    implicit val ec = system.dispatcher

    val lines = (1 until 50)
      .map(l => s"line$l").flatMap(l => if(l equals "line2"){ List(l, "    exception", "    exception", "    exception", "    exception", "    exception", "    exception") } else { List(l) } )
      .map(l => Json.obj("message" -> l))
      .asJavaCollection

    Source(lines.toIterator)
      .transform("multiline", MultilineFilter(pattern = "    .*"))
      .foreach(println)
      .onComplete(_ => system.shutdown())

    Thread.sleep(2000L)

  }
//
//
//  "test subsc" should "" in {
//
//    val redis = Redis(port=6379, host="localhost")
//
//    val filename = "test.txt"
//    val folder: File = Files.createTempDir()
//    val file: File = new File(folder, filename)
//
//    implicit val system = ActorSystem("Sys")
//    implicit val materializer = FlowMaterializer()
//    implicit val ec = system.dispatcher
//
//    Source(FilePublisher(folder.getAbsolutePath, List(filename)))
//      .runWith(SubscriberSink(RedisOutput()))
//
//
//    Thread.sleep(5000L)
//    file.createNewFile() shouldBe true
//
//    val lines = (1 until 50).map(l => s"line$l").flatMap(l => if(l equals "line2"){ List(l, "    exception", "    exception", "    exception", "    exception", "    exception", "    exception") } else { List(l) } ).asJavaCollection
//    FileUtils.writeLines(file, lines)
//
//    Source(RedisPublisher())
//      //Maj de la date
//      .transform("test", () => new stream.Transformer[JsValue, JsValue] {
//        var currentLine = ""
//        var currentJson = Json.obj()
//        override def onNext(element: JsValue): Seq[JsValue] = {
//          val line: String = (element \ "message").as[String]
//          if(line.startsWith(" ")){
//            currentLine = currentLine + "\n" + line
//            currentJson = currentJson.as[JsObject] ++ Json.obj("message" -> currentLine)
//            Seq()
//          }else{
//            val values: Seq[JsValue] = if(currentJson.values.isEmpty){
//              Seq()
//            } else {
//              Seq(element)
//            }
//            currentLine = line
//            currentJson = element.as[JsObject]
//            values
//          }
//        }
//      })
//      .map(json => json.as[JsObject] ++ Json.obj("@timestamp" -> new Date().getTime))
//      //.map(json => json.as[JsObject] ++ Json.obj("message" ->  s"${(json \ "message").as[String]} modified" ) )
//      .runWith(SubscriberSink(ElasticSearchOutput("localhost", 9200)))
//
//    Thread.sleep(15000L)
//    file.delete()
//  }


}
