package com.adelegue.reactive.logstash.input

import akka.actor._
import akka.stream.actor.ActorPublisher
import com.adelegue.reactive.logstash.input.impl.FolderWatcherActor._
import org.reactivestreams.{Publisher, Subscriber}
import play.api.libs.json.JsObject

object FilePublisher {

  def apply()(implicit actorSystem: ActorSystem) = new FilePublisherBuilder(actorSystem, 500, None, List())

  def apply(path: String)(implicit actorSystem: ActorSystem) = new FilePublisherBuilder(actorSystem, 500, Some(path), List())

}

class FilePublisher(actorSystem: ActorSystem, path: String, files: List[FileInfo], bufferSize: Int) extends Publisher[JsObject] {

  val actor = actorSystem.actorOf(FilePublisherMultiSubscriberActor.props(path, files, bufferSize))

  override def subscribe(subscriber: Subscriber[_ >: JsObject]): Unit = {
    actor ! FilePublisherMultiSubscriberActor.Subscribe(subscriber)
  }
}

class FilePublisherBuilder(actorSystem: ActorSystem, bufferSize: Int, path: Option[String], files: List[FileInfo], multipleSubscriber: Boolean = false) {

  def withMultipleSubscriber() = new FilePublisherBuilder(actorSystem, bufferSize, path, files, true)

  def withOneSubscriber() = new FilePublisherBuilder(actorSystem, bufferSize, path, files, false)

  def withFolder(folder: String) = new FilePublisherBuilder(actorSystem, bufferSize, Some(folder), files)

  def withFile(fileName: String) = new FilePublisherBuilder(actorSystem, bufferSize, path, FileInfo(fileName) :: files)

  def withBufferSize(bufferSize: Int) = new FilePublisherBuilder(actorSystem, bufferSize, path, files)

  def publisher(): Publisher[JsObject] = path match {
    case None         => throw new RuntimeException("Path missing")
    case Some(folder) =>
      if(multipleSubscriber){
        new FilePublisher(actorSystem, folder, files, bufferSize)
      } else {
        val ref = actorSystem.actorOf(FilePublisherOneSubscriberActor.props(folder, files))
        ActorPublisher[JsObject](ref)
      }
  }
}





