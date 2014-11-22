package com.adelegue.reactive.logstash.input

import akka.actor.ActorSystem
import akka.stream.scaladsl.Source
import com.adelegue.reactive.logstash.codec.Codec
import com.adelegue.reactive.logstash.input.publisher.FilePublisher
import play.api.libs.json.JsValue

/**
 * Created by adelegue on 18/11/14.
 */

case class FileInputArgs(path: String, files: List[String], bufferSize: Int = 500, multipleSubscriber: Boolean = false) extends Args


object FileInput {

  def apply(args: FileInputArgs, codec: Codec[JsValue, JsValue])(implicit actorSystem: ActorSystem): Source[JsValue]#Repr[JsValue] = {
    val filePublisher = FilePublisher(args.path, args.files, args.bufferSize, args.multipleSubscriber)
    Source(filePublisher).map(codec.apply)
  }

}


