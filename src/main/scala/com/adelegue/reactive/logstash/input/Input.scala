package com.adelegue.reactive.logstash.input

import akka.actor.ActorSystem
import akka.stream.scaladsl.Source
import com.adelegue.reactive.logstash.codec.Codec
import play.api.libs.json.JsValue

/**
 * Created by adelegue on 18/11/14.
 */


trait Args{

}

trait Input {
  def apply[I](args: Args, codec: Codec[I, JsValue])(implicit actorSystem: ActorSystem): Source[I]#Repr[JsValue]
}
