package com.adelegue.reactive.logstash.output

import java.util.concurrent.atomic.AtomicReference

import org.reactivestreams.{Subscription, Subscriber}

import scala.concurrent.Future

/**
 * Created by adelegue on 22/11/14.
 */
trait OutputSubscriber[T] extends Subscriber[T] {

  var subscriptionRef: AtomicReference[Subscription] = new AtomicReference[Subscription]()

  def output(message: T): Future[Any]

  override def onSubscribe(subscription: Subscription): Unit = {
    subscription.request(10)
    subscriptionRef.set(subscription)
  }

  override def onError(throwable: Throwable): Unit = {

  }

  override def onComplete(): Unit = {

  }

  override def onNext(t: T): Unit = {
    output(t)
  }
}
