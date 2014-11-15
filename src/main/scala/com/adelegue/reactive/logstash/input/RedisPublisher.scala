package com.adelegue.reactive.logstash.input

import akka.actor._
import akka.stream.actor.ActorPublisher
import com.adelegue.reactive.logstash.input.RedisPublisher.Poll
import com.adelegue.reactive.logstash.input.impl.{ FolderWatcherActor, ActorBufferPublisher, BufferActor }
import org.reactivestreams.Publisher
import play.api.libs.json.{ JsValue, Json }
import scredis.Redis

import scala.concurrent.duration.DurationDouble

/**
 * Created by adelegue on 14/11/2014.
 */
object RedisPublisher {
  case object Poll

  def props(redis: Redis, buffer: ActorRef) = Props(classOf[RedisPublisherActor], redis, buffer)

  def apply()(implicit actorSystem: ActorSystem): Publisher[JsValue] = apply(Redis())(actorSystem)

  def apply(redis: Redis)(implicit actorSystem: ActorSystem): Publisher[JsValue] = {
    val buffer = actorSystem.actorOf(ActorBufferPublisher.props())
    actorSystem.actorOf(RedisPublisher.props(redis, buffer))
    ActorPublisher[JsValue](buffer)
  }

}

class RedisPublisherActor(redis: Redis, buffer: ActorRef) extends Actor with ActorLogging {

  implicit val ec = context.dispatcher

  override def preStart(): Unit = {
    self ! Poll
  }

  def receive = {
    case Poll =>
      redis.rPop[String]("queue").map(_.map(Json.parse)) foreach {
        case Some(json) =>
          buffer ! BufferActor.Entry(json)
          self ! Poll
        case None =>
          retryLater()
      }

  }

  def retryLater(): Unit = {
    implicit val ctx = context.system.dispatcher
    context.system.scheduler.scheduleOnce(500 millisecond, self, Poll)(ctx)

  }

}