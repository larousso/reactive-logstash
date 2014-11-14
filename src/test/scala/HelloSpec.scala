import java.io.File
import java.util
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}

import akka.actor.ActorSystem
import akka.stream.scaladsl2.{FlowMaterializer, Source}
import com.adelegue.reactive.logstash.input.FilePublisher
import com.google.common.io.Files
import org.apache.commons.io.FileUtils
import org.reactivestreams.{Publisher, Subscriber, Subscription}
import org.scalatest._
import play.api.libs.json.JsObject

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.concurrent.{Future, Promise}

class HelloSpec extends FlatSpec with Matchers {

  "Hello" should "have tests" in {
    true should be === true
  }

  "test subsc" should "" in {

    val filename = "test.txt"
    val folder: File = Files.createTempDir()
    val file: File = new File(folder, filename)

    implicit val system = ActorSystem("Sys")
    val publisher: Publisher[JsObject] = FilePublisher().withOneSubscriber().withFolder(folder.getAbsolutePath).withFile(filename).publisher()


    Thread.sleep(5000L)
    file.createNewFile() shouldBe true

    val lines = (1 until 50).map(l => s"line$l").asJavaCollection
    FileUtils.writeLines(file, lines)

    implicit val materializer = FlowMaterializer()
    implicit val ec = system.dispatcher

    Source(publisher)
      .map(json => (json \ "message").as[String])
      .foreach(println)(materializer)
      .onComplete(_ => system.shutdown())
//
//    Source(publisher)
//      .map(json => (json \ "message").as[String])
//      .foreach(println)(materializer)
//      .onComplete(_ => system.shutdown())

    Thread.sleep(30000L)
    file.delete()
  }

  case class TestSubcriber(callback: JsObject => Unit) extends Subscriber[JsObject] {

    var aSubscription: AtomicReference[Subscription] = new AtomicReference[Subscription]()

    val futureSubscription = Promise[Subscription]()

    val promiseOnComplete = Promise[Unit]()

    val aFutureLine = Promise[JsObject]()

    var lines: util.List[JsObject] = new util.ArrayList[JsObject]()

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

    override def onNext(p1: JsObject): Unit = {
      callback(p1)
      lines.add(p1)
      newLine.set(true)
    }

    def hasNewLine: Boolean = {
      newLine.getAndSet(false)
    }

    def subscription(): Future[Subscription] = futureSubscription.future

    def line(): Future[JsObject] = aFutureLine.future

    def printLines(): Unit = {
      println(s"Lines : [${lines.map(json => (json \ "message").as[String] ).mkString(", ")}]")
      //println(s"Lines : [${lines.mkString(", ")}]")
    }

  }

}
