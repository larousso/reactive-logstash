package com.adelegue.reactive.logstash.input

import akka.actor._
import akka.stream.actor.ActorPublisher
import com.adelegue.reactive.logstash.input.impl.FolderWatcherActor.FileInfo
import com.adelegue.reactive.logstash.input.impl.{BufferActor, FolderWatcherActor}
import play.api.libs.json.JsObject

import scala.annotation.tailrec

class FilePublisherOneSubscriber {

}

object FilePublisherOneSubscriberActor {
  def props(folder: String, files: Seq[FileInfo]) = Props(classOf[FilePublisherOneSubscriberActor], folder, files)
}

class FilePublisherOneSubscriberActor(folder: String, files: Seq[FileInfo]) extends ActorPublisher[JsObject] with ActorLogging {
  import akka.stream.actor.ActorPublisherMessage._


  override def preStart(): Unit = {
    super.preStart()
    log.debug(s"Initializing folder watcher with $self")
    val ref = context.actorOf(FolderWatcherActor.props(self, folder, files))
    context.watch(ref)
  }

  var buf = Vector.empty[JsObject]

  def receive = {
    case BufferActor.Entry(i) =>
      log.debug(s"New entry $i")
      if (buf.isEmpty && totalDemand > 0)
        onNext(i)
      else {
        buf :+= i
        deliverBuf()
      }
    case Request(_) =>
      deliverBuf()
    case Cancel ⇒
      context.stop(self)
    case Terminated(ref) =>
      log.debug(s"Watcher terminated")
    case msg =>
      log.debug(s"Non handled message $msg")
  }

  @tailrec
  final def deliverBuf(): Unit =
    if (totalDemand > 0) {
      if (totalDemand <= Int.MaxValue) {
        val (use, keep) = buf.splitAt(totalDemand.toInt)
        buf = keep
        use foreach onNext
      } else {
        val (use, keep) = buf.splitAt(Int.MaxValue)
        buf = keep
        use foreach onNext
        deliverBuf()
      }
    }
}


