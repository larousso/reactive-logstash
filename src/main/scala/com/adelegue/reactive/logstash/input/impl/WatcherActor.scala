package com.adelegue.reactive.logstash.input.impl

import java.io.{RandomAccessFile, File}
import java.nio.file.StandardWatchEventKinds._
import java.nio.file.{WatchKey, WatchService, Path, Paths}
import java.util.Date

import akka.actor.SupervisorStrategy.Escalate
import akka.actor._
import play.api.libs.json.Json

import scala.concurrent.duration.DurationInt
import collection.JavaConversions._
import scala.util.{Failure, Success, Try}

/**
 * Created by adelegue on 14/11/2014.
 */

object FolderWatcherActor {
  case class Init()
  case object Run
  case object Next
  case object Process
  case object Stop
  case class FileInfo(expression: String)

  def props(buffer: ActorRef, folder: String, files: Seq[FolderWatcherActor.FileInfo]) = Props(classOf[FolderWatcherActor], buffer, folder, files, FileReaderActor)
}

private class FolderWatcherActor(buffer: ActorRef, folder: String, files: Seq[FolderWatcherActor.FileInfo], fileReaderProvider: FileReaderActorProvider) extends Actor with ActorLogging {

  override def preStart(): Unit = {
    self ! FolderWatcherActor.Init()
    log.debug(s"FolderWatcherActor : starting with buffer $buffer")
  }

  override val supervisorStrategy = OneForOneStrategy(maxNrOfRetries = 1, withinTimeRange = 1 minute) {
    case _ => Escalate
  }

  override def receive: Actor.Receive = pending(folder, files)

  def pending(path: String, files: Seq[FolderWatcherActor.FileInfo]): Receive = {
    case FolderWatcherActor.Init() =>
      val theFile: File = new File(path)

      if (theFile.isFile) {
        log.debug(s"Watching single file : ${theFile.getAbsolutePath}")
        context.become(watchFiles(Paths.get(theFile.getParent), fileHandled(Seq(FolderWatcherActor.FileInfo(theFile.getName)))))
      } else {
        files match {
          case Nil | Seq() =>
            log.debug(s"Watching all the folder : ${theFile.getAbsolutePath}")
            context.become(watchFiles(Paths.get(path), fileHandled(Seq(FolderWatcherActor.FileInfo(".*")))))
          case _ =>
            log.debug(s"Watching $files in the folder : ${theFile.getAbsolutePath}")
            context.become(watchFiles(Paths.get(path), fileHandled(files)))
        }
        self ! FolderWatcherActor.Run
      }
  }

  def fileHandled(patterns: Seq[FolderWatcherActor.FileInfo]): String => Boolean = {
    val regexPatterns = patterns.map(info => info.expression.r)

    def testFilename(filename: String): Boolean = {
      regexPatterns.filter(r => r.pattern.matcher(filename).matches()).nonEmpty
    }
    testFilename
  }

  def watchFiles(folder: Path, handled: String => Boolean): Receive = {

    case FolderWatcherActor.Run =>
      log.debug(s"Starting the watcher background task ")
      Try(folder.getFileSystem.newWatchService()) match {

        case Success(watcher) =>
          folder.register(watcher, ENTRY_CREATE, ENTRY_DELETE, ENTRY_MODIFY)
          context.become(running(watcher, folder, handled))
          self ! FolderWatcherActor.Next

        case Failure(e) =>
          log.error(s"Erreur sur le watch service", e)
          throw e
      }
  }

  def running(watcher: WatchService, folder: Path, handled: String => Boolean): Receive = {

    case FolderWatcherActor.Next =>
      implicit val ctx = context.system.dispatcher
      context.system.scheduler.scheduleOnce(500 millisecond, self, FolderWatcherActor.Process)(ctx)

    case FolderWatcherActor.Process =>
      pollEvents(watcher, folder, handled)
      self ! FolderWatcherActor.Next

    case FolderWatcherActor.Stop =>
      self ! PoisonPill

    case Terminated(ref) =>
      log.error(s"$ref terminated")
      if (context.children.size equals 0) {
        self ! PoisonPill
      }
  }

  def createFileListener(folder: Path, filename: String, buffer: ActorRef): ActorRef = {
    val props: Props = fileReaderProvider.props(buffer)
    val ref = context.actorOf(props, filename)
    context.watch(ref)
    ref ! FileReaderActor.Start(new File(folder.toFile.getAbsolutePath, filename))
    ref
  }

  private def pollEvents(watcher: WatchService, folder: Path, handled: String => Boolean): Unit = {
    val key: WatchKey = watcher.poll() // blocks
    if (key != null) {
      key.pollEvents().groupBy(e => (e.kind(), e.context().toString)).foreach { event =>
        val (kind, filename) = event._1
        if (kind == ENTRY_CREATE) {
          log.debug(s"$filename created")
          if (handled(filename)) {
            val reader = createFileListener(folder, filename, buffer)
            reader ! FileReaderActor.FileChange
          }
        }
        if (kind == ENTRY_DELETE) {
          log.debug(s"$filename deleted")
          context.child(filename) match {
            case Some(fileWatcher) => fileWatcher ! PoisonPill
            case _                 =>
          }
        }
        if (kind == ENTRY_MODIFY) {
          log.debug(s"$filename modified")

          context.child(filename) match {
            case Some(fileWatcher) =>
              fileWatcher ! FileReaderActor.FileChange
            case None =>
              if (handled(filename)) {
                val fileWatcher = createFileListener(folder, filename, buffer)
                fileWatcher ! FileReaderActor.FileChange
              }
          }
        }
      }
      key.reset()
    }
  }

}

trait FileReaderActorProvider {
  def props(buffer: ActorRef): Props = Props(classOf[FileReaderActor], buffer)
}

private object FileReaderActor extends FileReaderActorProvider {
  case class Start(file: File)
  case object FileChange
}

private class FileReaderActor(buffer: ActorRef) extends Actor with ActorLogging {

  override def receive: Receive = pending

  def pending: Receive = {

    case FileReaderActor.Start(file) =>
      Try(new RandomAccessFile(file, "r")) match {

        case Success(reader) =>
          log.info(s"starting tail on file ${file.getAbsolutePath} with buffer $buffer")
          context.become(running(file, reader, 0))

        case Failure(error) =>
          error.printStackTrace()
          log.error(s"Error creating reader for $file", error)
      }
  }

  def running(file: File, reader: RandomAccessFile, position: Long): Receive = {

    case FileReaderActor.FileChange =>
      log.debug(s"File $file changes, reading lines from $position to ...")
      val newLines = readFile(reader, position)
      newLines
        .map{l =>
        Json.obj(
          "@timestamp" -> new Date().getTime,
          "message" -> l,
          "file" -> file.getAbsolutePath)
      }
        .foreach{line =>
        buffer ! BufferActor.Entry(line)
      }
      context.become(running(file, reader, reader.getFilePointer))
  }

  def readFile(reader: RandomAccessFile, position: Long): List[String] = {
    reader.seek(position)
    read(reader)
  }

  def read(reader: RandomAccessFile): List[String] = {
    reader.readLine() match {

      case null => List()

      case line => line :: read(reader)
    }
  }
}
