import java.io.File
import java.util
import java.util.concurrent.atomic.{ AtomicBoolean, AtomicReference }

import akka.actor.ActorSystem
import akka.stream.scaladsl2.Source
import com.adelegue.reactive.logstash.input.{ FilesTailer }
import com.google.common.io.Files
import org.apache.commons.io.FileUtils
import org.reactivestreams.{ Publisher, Subscriber, Subscription }
import org.scalatest._
import play.api.libs.json.JsObject

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.concurrent.duration.DurationLong
import scala.concurrent.{ Await, Future, Promise }

class HelloSpec extends FlatSpec with Matchers {

  "Hello" should "have tests" in {
    true should be === true
  }

  "test subsc" should "" in {

    val filename = "test.txt"
    val folder: File = Files.createTempDir()
    val file: File = new File(folder, filename)

    implicit val system = ActorSystem("Sys")
    val publisher: Publisher[JsObject] = FilesTailer().withFolder(folder.getAbsolutePath).withFile(filename).publisher()
    val subsc1 = TestSubcriber(l => println(s"subsc1 $l"))
    val subsc2 = TestSubcriber(l => println(s"subsc2 $l"))
    publisher.subscribe(subsc1)
    publisher.subscribe(subsc2)

    val subscription1 = Await.result(subsc1.subscription(), 1 second)
    println(s"Subscription1 :  $subscription1")
    val subscription2 = Await.result(subsc2.subscription(), 1 second)
    println(s"Subscription2 :  $subscription2")
    subscription1.request(5)
    subscription1.request(15)
    subscription2.request(10)
    Thread.sleep(5000L)
    file.createNewFile() shouldBe true

    val lines = (1 until 50).map(l => s"line$l").asJavaCollection
    FileUtils.writeLines(file, lines)
    file.delete()
    subscription2.request(10)

    //Source(publisher).

    Thread.sleep(20000L)

    subsc1.printLines()
    subsc2.printLines()


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
    }

  }

}
