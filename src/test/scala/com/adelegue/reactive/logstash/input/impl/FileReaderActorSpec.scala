package com.adelegue.reactive.logstash.input.impl

import java.io.File

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestActorRef, TestKit, TestProbe}
import com.adelegue.reactive.logstash.input.Line
import com.google.common.io.Files
import org.apache.commons.io.FileUtils
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

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
      val ref = TestActorRef(FileReaderActor.props(buffer.ref))

      ref ! FileReaderActor.Start(file)

      ref ! FileReaderActor.FileChange

      buffer.expectMsg(BufferActor.Entry(Line("line1", file.getAbsolutePath)))
      buffer.expectMsg(BufferActor.Entry(Line("line2", file.getAbsolutePath)))
      buffer.expectMsg(BufferActor.Entry(Line("line3", file.getAbsolutePath)))
      buffer.expectMsg(BufferActor.Entry(Line("line4", file.getAbsolutePath)))

      val newLines: List[String] = List("line5", "line6", "line7", "line8")
      FileUtils.writeLines(file, newLines.asJavaCollection, true)

      Source.fromFile(file).getLines().toList shouldBe strings ::: newLines

      ref ! FileReaderActor.FileChange

      buffer.expectMsg(BufferActor.Entry(Line("line5", file.getAbsolutePath)))
      buffer.expectMsg(BufferActor.Entry(Line("line6", file.getAbsolutePath)))
      buffer.expectMsg(BufferActor.Entry(Line("line7", file.getAbsolutePath)))
      buffer.expectMsg(BufferActor.Entry(Line("line8", file.getAbsolutePath)))

    }
  }
}