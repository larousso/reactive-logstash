package com.adelegue.reactive.logstash.input.publisher

import akka.actor.ActorRef

/**
 * Created by adelegue on 23/12/14.
 */
object Messages {

  case class Init(buffer: ActorRef)
}
