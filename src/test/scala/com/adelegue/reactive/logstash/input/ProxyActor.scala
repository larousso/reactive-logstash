package com.adelegue.reactive.logstash.input

import akka.actor.{ ActorLogging, Actor, ActorRef }

/**
 * Created by adelegue on 03/11/2014.
 */
class ProxyActor(actorRef: ActorRef) extends Actor with ActorLogging {

  override def preStart(): Unit = {
    log.debug(s"proxy starting")
  }

  override def receive: Receive = {
    case msg =>
      log.debug(s"forwarding message $msg to $actorRef")
      actorRef forward msg
  }

}
