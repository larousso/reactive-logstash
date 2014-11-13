package com.adelegue.reactive.logstash.input

import java.io.{ File, RandomAccessFile }
import java.nio.file.StandardWatchEventKinds._
import java.nio.file._
import java.util.Date

import akka.actor.SupervisorStrategy.Escalate
import akka.actor._
import com.adelegue.reactive.logstash.input.FolderWatcherActor.FileInfo
import org.reactivestreams.{ Publisher, Subscriber, Subscription }
import play.api.libs.json.{Json, JsNumber, JsString, JsObject}

import scala.collection.JavaConversions._
import scala.concurrent.duration.DurationDouble
import scala.util.{ Failure, Success, Try }

object FilesTailer {

  def apply()(implicit actorSystem: ActorSystem) = new FilesTailerBuilder(actorSystem, 500, None, List())

  def apply(path: String)(implicit actorSystem: ActorSystem) = new FilesTailerBuilder(actorSystem, 500, Some(path), List())

}

class FilesTailer(actorSystem: ActorSystem, path: String, files: List[FileInfo], bufferSize: Int) extends Publisher[JsObject] {

  val actor = actorSystem.actorOf(FilesTailerPublisherActor.props(path, files, bufferSize))

  override def subscribe(subscriber: Subscriber[_ >: JsObject]): Unit = {
    actor ! FilesTailerPublisherActor.Subscribe(subscriber)
  }
}

class FilesTailerBuilder(actorSystem: ActorSystem, bufferSize: Int, path: Option[String], files: List[FileInfo]) {

  def withFolder(folder: String) = new FilesTailerBuilder(actorSystem, bufferSize, Some(folder), files)

  def withFile(fileName: String) = new FilesTailerBuilder(actorSystem, bufferSize, path, FileInfo(fileName) :: files)

  def withBufferSize(bufferSize: Int) = new FilesTailerBuilder(actorSystem, bufferSize, path, files)

  def publisher(): Publisher[JsObject] = path match {
    case None         => throw new RuntimeException("Path missing")
    case Some(folder) => new FilesTailer(actorSystem, folder, files, bufferSize)
  }
}

object FilesTailerPublisherActor {

  case class Subscribe(subscriber: Subscriber[_ >: JsObject])

  def props(path: String, files: List[FileInfo], bufferSize: Int) = Props(classOf[FilesTailerPublisherActor], path, files, bufferSize)
}

class FilesTailerPublisherActor(path: String, files: List[FileInfo], bufferSize: Int) extends Actor with ActorLogging {

  val buffer = context.actorOf(BufferActor.props(bufferSize))
  context.watch(buffer)
  private val watcher: ActorRef = context.actorOf(FolderWatcherActor.props(buffer, path, files))
  context.watch(watcher)

  override def receive = running(List())

  def running(subscribers: List[(ActorRef, Subscriber[_ >: JsObject])]): Receive = {

    case FilesTailerPublisherActor.Subscribe(subscriber) =>
      log.debug(s"Subscribing $subscriber")

      if (subscribers.exists(_._2 equals subscriber)) {
        subscriber.onError(new IllegalStateException(s"can not subscribe the same subscriber multiple times (see reactive-streams specification, rules 1.10 and 2.12"))
      } else {
        val subscriptionActorRef = context.actorOf(BufferSubscriptionActor.props(buffer, subscriber))
        context.watch(subscriptionActorRef)
        subscriber.onSubscribe(BufferSubscription(subscriber, subscriptionActorRef))
        context.become(running((subscriptionActorRef, subscriber) :: subscribers))
      }

    case Terminated(ref) if ref equals buffer =>
      //TODO créer un erreur dédiée.
      subscribers.foreach(_._2.onError(new IllegalStateException))

    case Terminated(ref) if ref equals watcher =>
      subscribers.foreach(s => s._1 ! BufferSubscriptionActor.Complete)

    case Terminated(ref) =>
      context.become(running(subscribers.filterNot(_._1 equals ref)))

    case _ =>
      context.become(onErrorState(List()))
      subscribers.foreach { s =>
        //TODO créer un erreur dédiée.
        s._2.onError(new IllegalStateException)
        s._1 ! PoisonPill
      }
  }

  def onErrorState(subscribers: List[(ActorRef, Subscriber[_ >: JsObject])]): Receive = {
    //TODO créer un erreur dédiée.
    case FilesTailerPublisherActor.Subscribe(subscriber) => subscriber.onError(new IllegalStateException)
    case any                                             => log.debug(s"Unhandled message $any")
  }

}

private object FolderWatcherActor {
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

  val bufferSize = 300

  override def receive: Receive = pending

  def pending: Receive = {

    case FileReaderActor.Start(file) =>
      Try(new RandomAccessFile(file, "r")) match {

        case Success(reader) =>
          log.info(s"starting tail on file ${file.getAbsolutePath}")
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
        .foreach(line => buffer ! BufferActor.Entry(line))
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
