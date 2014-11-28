package com.adelegue.reactive.logstash.output

import java.util.concurrent.atomic.AtomicReference

import org.reactivestreams.{Subscription, Subscriber}

import scala.concurrent.Future

/**
 * Created by adelegue on 22/11/14.
 */
trait OutputSubscriber[T] extends Subscriber[T] {

  var subscriptionRef: AtomicReference[Subscription] = new AtomicReference[Subscription]()

  val nbRequest = 10
  var current = 0

  def output(message: T): Future[Any]

  override def onSubscribe(subscription: Subscription): Unit = {
    subscriptionRef.set(subscription)
    doRequest()
  }

  override def onError(throwable: Throwable): Unit = {

  }

  override def onComplete(): Unit = {

  }

  override def onNext(t: T): Unit = {
    output(t)
    current -= 1
    doRequest()
  }

  def doRequest(): Unit = {
    if(current == 0) {
      subscriptionRef.get().request(nbRequest)
      current = nbRequest
    }
  }
}
