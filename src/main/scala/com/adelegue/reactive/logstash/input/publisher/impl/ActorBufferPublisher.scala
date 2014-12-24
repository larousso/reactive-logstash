package com.adelegue.reactive.logstash.input.publisher.impl

import akka.actor._
import akka.stream.actor.ActorPublisher
import com.adelegue.reactive.logstash.input.publisher.Messages
import play.api.libs.json.JsValue

import scala.annotation.tailrec

object ActorBufferPublisher {
  def props(ref: ActorRef) = Props(classOf[ActorBufferPublisher], ref)
}

class ActorBufferPublisher(ref: ActorRef) extends ActorPublisher[JsValue] with ActorLogging {
  import akka.stream.actor.ActorPublisherMessage._

  var buf = Vector.empty[JsValue]

  def receive = pending

  def pending : Actor.Receive = {
    case Request(req) =>
      ref ! Messages.Init(self)
      context.become(running)
  }

  def running : Actor.Receive = {
    case BufferActor.Entry(json) =>
      if (buf.isEmpty && totalDemand > 0)
        onNext(json)
      else {
        log.debug(s"To buffer $json")
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

