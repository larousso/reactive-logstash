package com.adelegue.reactive.logstash.input.publisher

import akka.actor._
import akka.stream.actor.ActorPublisher
import com.adelegue.reactive.logstash.input.publisher.impl.{ActorBufferPublisher, FolderWatcherActor}
import FolderWatcherActor._
import org.reactivestreams.{Publisher, Subscriber}
import play.api.libs.json.JsValue

object FilePublisher {

  def apply(path: String, files: List[String], bufferSize: Int = 500)(implicit actorSystem: ActorSystem): Publisher[JsValue] = {
    val fileRef = actorSystem.actorOf(FolderWatcherActor.props (path, files.map(FileInfo)))
    val publisherRef = actorSystem.actorOf(ActorBufferPublisher.props(fileRef))
    ActorPublisher[JsValue] (publisherRef)
  }
}
