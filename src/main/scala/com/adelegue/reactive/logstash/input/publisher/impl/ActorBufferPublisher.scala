package com.adelegue.reactive.logstash.input.publisher.impl

import akka.actor._
import akka.stream.actor.ActorPublisher
import play.api.libs.json.JsValue

import scala.annotation.tailrec

object ActorBufferPublisher {
  def props() = Props(classOf[ActorBufferPublisher])
}

class ActorBufferPublisher() extends ActorPublisher[JsValue] with ActorLogging {
  import akka.stream.actor.ActorPublisherMessage._

  var buf = Vector.empty[JsValue]

  def receive = {
    case BufferActor.Entry(json) =>
      //log.debug(s"New entry $json")
      if (buf.isEmpty && totalDemand > 0)
        onNext(json)
      else {
        log.debug(s"To buffer ...")
        buf :+= json
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
        log.debug(s"Read $totalDemand from buffer ...")
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

