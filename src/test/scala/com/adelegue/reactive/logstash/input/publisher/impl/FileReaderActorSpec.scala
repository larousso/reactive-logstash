package com.adelegue.reactive.logstash.input.publisher.impl

import java.io.File

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import com.adelegue.reactive.logstash.input.publisher.impl.BufferActor.Entry
import com.google.common.io.Files
import org.apache.commons.io.FileUtils
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import play.api.libs.json.JsValue

import scala.collection.JavaConverters._
import scala.io.Source

/**
 * Created by adelegue on 04/11/2014.
 */
class FileReaderActorSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender with WordSpecLike with Matchers with BeforeAndAfterAll {

  def this() = this(ActorSystem("FileReaderActorSpec"))

  override def afterAll() {
    TestKit.shutdownActorSystem(system)
  }

  "File Reader " must {

    "read line in the right order" in {
      val folder = Files.createTempDir()
      val filename: String = "testReadLines.txt"

      val file = new File(folder.getAbsolutePath, filename)
      val strings: List[String] = List("line1", "line2", "line3", "line4")
      FileUtils.writeLines(file, strings.asJavaCollection)

      val buffer = TestProbe()
      val ref = system.actorOf(FileReaderActor.props(buffer.ref, file))

      ref ! FileReaderActor.FileChange

      buffer.expectMsgPF(){
        case Entry(json) => validateJson(json, "line1", file.getAbsolutePath)
        case msg => println(msg)
      }
      buffer.expectMsgPF(){
        case Entry(json) => validateJson(json, "line2", file.getAbsolutePath)
      }
      buffer.expectMsgPF(){
        case Entry(json) => validateJson(json, "line3", file.getAbsolutePath)
      }
      buffer.expectMsgPF(){
        case Entry(json) => validateJson(json, "line4", file.getAbsolutePath)
      }

      val newLines: List[String] = List("line5", "line6", "line7", "line8")
      FileUtils.writeLines(file, newLines.asJavaCollection, true)

      Source.fromFile(file).getLines().toList shouldBe strings ::: newLines

      ref ! FileReaderActor.FileChange
      buffer.expectMsgPF(){
        case Entry(json) => validateJson(json, "line5", file.getAbsolutePath)
      }
      buffer.expectMsgPF(){
        case Entry(json) => validateJson(json, "line6", file.getAbsolutePath)
      }
      buffer.expectMsgPF(){
        case Entry(json) => validateJson(json, "line7", file.getAbsolutePath)
      }
      buffer.expectMsgPF(){
        case Entry(json) => validateJson(json, "line8", file.getAbsolutePath)
      }

    }
  }

  def validateJson(json: JsValue, message: String, file: String): Unit = {
    (json \ "message").as[String] shouldBe message
    (json \ "file").as[String] shouldBe file
  }
}