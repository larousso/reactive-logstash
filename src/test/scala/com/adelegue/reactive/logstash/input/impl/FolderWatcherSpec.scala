package com.adelegue.reactive.logstash.input.impl

import java.io.{File, PrintWriter}

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestActorRef, TestKit, TestProbe}
import com.adelegue.reactive.logstash.input.ProxyActor
import com.adelegue.reactive.logstash.input.impl.FolderWatcherActor.FileInfo
import com.google.common.io.Files
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.duration.DurationInt

/**
 * Created by adelegue on 03/11/2014.
 */
class FolderWatcherSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender with WordSpecLike with Matchers with BeforeAndAfterAll {

  def this() = this(ActorSystem("FileWatcherSpec"))

  override def afterAll() {
    TestKit.shutdownActorSystem(system)
    val file = new File(System.getProperty("java.io.tmpdir"), "test.txt")
    file.delete()
  }

  "File watcher " must {

    "send the rigth notifications" in {

      val buffer = TestProbe()
      val filename: String = "test.txt"
      val folder = Files.createTempDir()

      val file = new File(folder.getAbsolutePath, filename)

      val probe = TestProbe()

      val ref = TestActorRef(Props(classOf[FolderWatcherActor], buffer.ref, folder.getAbsolutePath, Seq(FileInfo(filename)), MockFileReader(probe.ref)))
      println(s"creating file in ${file.getAbsolutePath}")
      val watcher = TestProbe()
      watcher.watch(ref)

      val created = file.createNewFile()
      created shouldBe true
      //probe.expectMsg(60 second, FileReaderActor.Start(file))
      probe.expectMsg(60 second, FileReaderActor.FileChange)

      val writer: PrintWriter = new PrintWriter(file.getAbsolutePath, "UTF-8")
      writer.println("toto")

      probe.expectMsg(60 second, FileReaderActor.FileChange)
      file.delete()
      watcher.expectTerminated(ref, 60 second)
    }

    "handle multiple file" in {
      val filename1: String = "test1.txt"
      val filename2: String = "test2.txt"
      val folder = Files.createTempDir()

      val file1 = new File(folder.getAbsolutePath, filename1)
      val file2 = new File(folder.getAbsolutePath, filename2)

      val buffer = TestProbe()
      val probe = TestProbe()

      val ref = TestActorRef(Props(classOf[FolderWatcherActor], buffer.ref, folder.getAbsolutePath, Seq(FileInfo(filename1), FileInfo(filename2)), MockFileReader(probe.ref)))
      val watcher = TestProbe()
      watcher.watch(ref)

      file1.createNewFile() shouldBe true
      file2.createNewFile() shouldBe true

      //probe.expectMsgAnyOf(60 second, FileReaderActor.Start(file1), FileReaderActor.Start(file2))
      //probe.expectMsgAnyOf(60 second, FileReaderActor.Start(file1), FileReaderActor.Start(file2), FileReaderActor.FileChange)
      //probe.expectMsgAnyOf(60 second, FileReaderActor.Start(file1), FileReaderActor.Start(file2), FileReaderActor.FileChange)
      probe.expectMsg(60 second, FileReaderActor.FileChange)
      probe.expectMsg(60 second, FileReaderActor.FileChange)

      file1.delete() shouldBe true
      file2.delete() shouldBe true

      watcher.expectTerminated(ref, 60 second)
    }

  }

}

object MockFileReader {
  def apply(ref: ActorRef) = new MockFileReader(ref)
}

class MockFileReader(ref: ActorRef) extends FileReaderActorProvider {
  override def props(buffer: ActorRef, file: File): Props = Props(classOf[ProxyActor], ref)
}
