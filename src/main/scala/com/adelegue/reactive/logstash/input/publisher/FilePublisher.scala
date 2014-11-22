package com.adelegue.reactive.logstash.input.publisher

import akka.actor._
import akka.stream.actor.ActorPublisher
import com.adelegue.reactive.logstash.input.publisher.impl.{ActorBufferPublisher, FolderWatcherActor}
import FolderWatcherActor._
import org.reactivestreams.{Publisher, Subscriber}
import play.api.libs.json.JsValue

object FilePublisher {

  def apply(path: String, files: List[String], bufferSize: Int = 500, multipleSubscriber: Boolean = false)(implicit actorSystem: ActorSystem): Publisher[JsValue] = {
    if (multipleSubscriber) {
      FilePublisherMultipleSubscriber(actorSystem, path, files.map(FileInfo), bufferSize)
    } else {
      FilePublisherOneSubscriber(actorSystem, path, files.map(FileInfo), bufferSize)
    }
  }
}


object FilePublisherOneSubscriber{
  def apply(actorSystem: ActorSystem, path: String, files: List[FileInfo], bufferSize: Int): Publisher[JsValue] = {
    val ref = actorSystem.actorOf(ActorBufferPublisher.props())
    actorSystem.actorOf(FolderWatcherActor.props (ref, path, files))
    ActorPublisher[JsValue] (ref)
  }
}

object FilePublisherMultipleSubscriber {
  def apply(actorSystem: ActorSystem, path: String, files: List[FileInfo], bufferSize: Int): Publisher[JsValue] = {
    new FilePublisherMultipleSubscriber(actorSystem, path, files, bufferSize)
  }
}

class FilePublisherMultipleSubscriber(actorSystem: ActorSystem, path: String, files: List[FileInfo], bufferSize: Int) extends Publisher[JsValue] {

  val actor = actorSystem.actorOf(FilePublisherMultiSubscriberActor.props(path, files, bufferSize))

  override def subscribe(subscriber: Subscriber[_ >: JsValue]): Unit = {
    actor ! FilePublisherMultiSubscriberActor.Subscribe(subscriber)
  }
}
