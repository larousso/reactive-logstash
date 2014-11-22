package com.adelegue.reactive.logstash.output

import org.reactivestreams.Subscriber
import play.api.libs.json.{ Json, JsValue }
import scredis.Redis

import scala.concurrent.{ExecutionContext, Future}

/**
 * Created by adelegue on 14/11/2014.
 */

object RedisOutput  {

  def apply()(implicit ec: ExecutionContext): Subscriber[JsValue] = new RedisOutput(Redis())(ec)

  def apply(redis: Redis)(implicit ec: ExecutionContext): Subscriber[JsValue] =  new RedisOutput(redis)(ec)

}
class RedisOutput(redis: Redis)(implicit ec: ExecutionContext) extends OutputSubscriber[JsValue] {

  override def output(message: JsValue): Future[Unit] = {
    redis.lPush[String]("queue", Json.stringify(message)).map(l => Unit)
  }
}
