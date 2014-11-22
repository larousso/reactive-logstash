import java.io.File
import java.util
import java.util.Date
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}

import akka.actor.ActorSystem
import akka.stream.FlowMaterializer
import akka.stream.scaladsl.Source
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
import scala.concurrent.{Future, Promise}

class MainSpec extends FlatSpec with Matchers {

  "test subsc" should "" in {

    val redis = Redis(port=6379, host="localhost")

    val filename = "test.txt"
    val folder: File = Files.createTempDir()
    val file: File = new File(folder, filename)

    implicit val system = ActorSystem("Sys")
    implicit val materializer = FlowMaterializer()
    implicit val ec = system.dispatcher

    Source(FilePublisher(folder.getAbsolutePath, List(filename)))
      .foreach(RedisOutput().apply(_).map(_ => Unit))


    Thread.sleep(5000L)
    file.createNewFile() shouldBe true

    val lines = (1 until 50).map(l => s"line$l").asJavaCollection
    FileUtils.writeLines(file, lines)

    Source(RedisPublisher())
      //Maj de la date
      .map(json => json.as[JsObject] ++ Json.obj("@timestamp" -> new Date().getTime))
      .map(json => json.as[JsObject] ++ Json.obj("message" ->  s"${(json \ "message").as[String]} modified" ) )
      .foreach(ElasticSearchOutput("localhost", 9200).apply(_).map(_ => ()))

      .onComplete(_ => system.shutdown())



    Thread.sleep(30000L)
    file.delete()
  }

  case class TestSubcriber(callback: JsValue => Unit) extends Subscriber[JsValue] {

    var aSubscription: AtomicReference[Subscription] = new AtomicReference[Subscription]()

    val futureSubscription = Promise[Subscription]()

    val promiseOnComplete = Promise[Unit]()

    val aFutureLine = Promise[JsValue]()

    var lines: util.List[JsValue] = new util.ArrayList[JsValue]()

    var newLine = new AtomicBoolean()

    override def onError(p1: Throwable): Unit = {
      println(p1)
    }

    override def onSubscribe(subscription: Subscription): Unit = {
      futureSubscription.success(subscription)
      this.aSubscription.set(subscription)
    }

    override def onComplete(): Unit = {
      promiseOnComplete.success(Unit)
    }

    def futureOnComplete(): Future[Unit] = promiseOnComplete.future

    override def onNext(p1: JsValue): Unit = {
      callback(p1)
      lines.add(p1)
      newLine.set(true)
    }

    def hasNewLine: Boolean = {
      newLine.getAndSet(false)
    }

    def subscription(): Future[Subscription] = futureSubscription.future

    def line(): Future[JsValue] = aFutureLine.future

    def printLines(): Unit = {
      println(s"Lines : [${lines.map(json => (json \ "message").as[String] ).mkString(", ")}]")
      //println(s"Lines : [${lines.mkString(", ")}]")
    }

  }

}
