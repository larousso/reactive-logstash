package tck

import java.io.File

import akka.actor.{Actor, ActorSystem, Props}
import com.adelegue.reactive.logstash.input.FilePublisher
import com.google.common.io.Files
import org.apache.commons.io.FileUtils
import org.reactivestreams.{Publisher, Subscriber, Subscription}
import org.reactivestreams.tck.{PublisherVerification, TestEnvironment}
import org.scalatest.testng.TestNGSuiteLike
import org.testng.annotations.AfterClass
import play.api.libs.json.JsObject

import scala.collection.JavaConverters._
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.util.Random

/**
 *
 * Created by adelegue on 05/11/2014.
 */
class FilesTailerPublisherTest(val system: ActorSystem, env: TestEnvironment, publisherShutdownTimeoutMillis: Long)
    extends PublisherVerification[JsObject](env, publisherShutdownTimeoutMillis)
    with TestNGSuiteLike {

  def this() {
    this(ActorSystem(), new TestEnvironment(10000, true), 1000)
  }

  val folder = Files.createTempDir()

  @AfterClass
  def cleanFiles(): Unit = {
    folder.listFiles().filter(file => file.getName.contains("line")).foreach(file => file.delete())
  }


  override def skipStochasticTests(): Boolean = true

  override def createPublisher(elements: Long): Publisher[JsObject] = {
    implicit val as = system
    val ramdom = Random.nextInt(10000)
    val fileName: String = s"test$elements-$ramdom.txt"
    val publisher: Publisher[JsObject] = FilePublisher(folder.getAbsolutePath).withFile(fileName).publisher()
    Thread.sleep(5000L)
    val lines = (0 until elements.toInt).map(i => s"line$i").asJavaCollection
    val file: File = new File(folder.getAbsolutePath, fileName)
    FileUtils.writeLines(file, lines)
    Thread.sleep(15000L)
    file.delete()
    publisher
  }

  def deleteFileIn(file: File, duration: FiniteDuration) = {
    val ref = system.actorOf(Props(classOf[FileDeleter]))
    system.scheduler.scheduleOnce(duration, ref, FileDeleter.Delete(file))(system.dispatcher)
  }

  override def createErrorStatePublisher(): Publisher[JsObject] = null

  @AfterClass
  def shutdownActorSystem(): Unit = {
    system.shutdown()
    system.awaitTermination(10 seconds)
  }
}

object FileDeleter {
  case class Delete(file: File)
}

class FileDeleter extends Actor {

  override def receive: Receive = {
    case FileDeleter.Delete(file) => file.delete()
  }
}
