package com.adelegue.reactive.logstash.input.publisher

import akka.actor._
import akka.stream.actor.ActorPublisher
import com.adelegue.reactive.logstash.input.publisher.RedisPublisher.Poll
import com.adelegue.reactive.logstash.input.publisher.impl.{ActorBufferPublisher, BufferActor}
import org.reactivestreams.Publisher
import play.api.libs.json.{JsValue, Json}
import scredis.Redis

import scala.concurrent.duration.DurationDouble

/**
 * Created by adelegue on 14/11/2014.
 */
object RedisPublisher {

  case object Poll

  def props(redis: Redis) = Props(classOf[RedisPublisherActor], redis)

  def apply()(implicit actorSystem: ActorSystem): Publisher[JsValue] = apply(Redis())(actorSystem)

  def apply(redis: Redis)(implicit actorSystem: ActorSystem): Publisher[JsValue] = {
    val ref = actorSystem.actorOf(RedisPublisher.props(redis))
    val buffer = actorSystem.actorOf(ActorBufferPublisher.props(ref))
    ActorPublisher[JsValue](buffer)
  }

}

class RedisPublisherActor(redis: Redis) extends Actor with ActorLogging {

  implicit val ec = context.dispatcher

  def receive = pending

  def pending: Actor.Receive = {
    case Messages.Init(buffer) =>
      context.become(running(buffer))
      self ! Poll
  }

  def running(buffer: ActorRef): Actor.Receive = {
    case Poll =>
      redis.rPop[String]("queue").map(_.map(Json.parse)) foreach {
        case Some(json) =>
          log.debug(s"Getting $json from REDIS")
          buffer ! BufferActor.Entry(json)
          self ! Poll
        case None =>
          retryLater()
      }
  }

  def retryLater(): Unit = {
    implicit val ctx = context.system.dispatcher
    context.system.scheduler.scheduleOnce(500 milliseconds, self, Poll)(ctx)

  }

}