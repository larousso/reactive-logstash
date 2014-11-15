package com.adelegue.reactive.logstash.input

import akka.actor._
import akka.stream.actor.ActorPublisher
import com.adelegue.reactive.logstash.input.impl.{ ActorBufferPublisher, FolderWatcherActor }
import com.adelegue.reactive.logstash.input.impl.FolderWatcherActor._
import org.reactivestreams.{ Publisher, Subscriber }
import play.api.libs.json.JsValue

object FilePublisher {

  def apply(path: String)(implicit actorSystem: ActorSystem) = new FilePublisherBuilder(actorSystem, 500, Some(path), List())

  def apply(path: String, files: List[String], bufferSize: Int = 500, multipleSubscriber: Boolean = false)(implicit actorSystem: ActorSystem) = {
    new FilePublisherBuilder(actorSystem, bufferSize, Some(path), files.map(FileInfo), multipleSubscriber).publisher()
  }

  def apply()(implicit actorSystem: ActorSystem) = new FilePublisherBuilder(actorSystem, 500, None, List())
}

class FilePublisher(actorSystem: ActorSystem, path: String, files: List[FileInfo], bufferSize: Int) extends Publisher[JsValue] {

  val actor = actorSystem.actorOf(FilePublisherMultiSubscriberActor.props(path, files, bufferSize))

  override def subscribe(subscriber: Subscriber[_ >: JsValue]): Unit = {
    actor ! FilePublisherMultiSubscriberActor.Subscribe(subscriber)
  }
}

class FilePublisherBuilder(actorSystem: ActorSystem, bufferSize: Int, path: Option[String], files: List[FileInfo], multipleSubscriber: Boolean = false) {

  def withMultipleSubscriber() = new FilePublisherBuilder(actorSystem, bufferSize, path, files, true)

  def withOneSubscriber() = new FilePublisherBuilder(actorSystem, bufferSize, path, files, false)

  def withFolder(folder: String) = new FilePublisherBuilder(actorSystem, bufferSize, Some(folder), files)

  def withFile(fileName: String) = new FilePublisherBuilder(actorSystem, bufferSize, path, FileInfo(fileName) :: files)

  def withBufferSize(bufferSize: Int) = new FilePublisherBuilder(actorSystem, bufferSize, path, files)

  def publisher(): Publisher[JsValue] = path match {
    case None => throw new RuntimeException("Path missing")
    case Some(folder) =>
      if (multipleSubscriber) {
        new FilePublisher(actorSystem, folder, files, bufferSize)
      } else {
        val ref = actorSystem.actorOf(ActorBufferPublisher.props())
        actorSystem.actorOf(FolderWatcherActor.props(ref, folder, files))
        ActorPublisher[JsValue](ref)
      }
  }
}

