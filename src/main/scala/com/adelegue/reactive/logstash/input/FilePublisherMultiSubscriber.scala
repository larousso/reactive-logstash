package com.adelegue.reactive.logstash.input

import akka.actor._
import com.adelegue.reactive.logstash.input.impl.FolderWatcherActor.FileInfo
import com.adelegue.reactive.logstash.input.impl.{ BufferActor, BufferSubscription, BufferSubscriptionActor, FolderWatcherActor }
import org.reactivestreams.Subscriber
import play.api.libs.json.JsValue

object FilePublisherMultiSubscriberActor {

  case class Subscribe(subscriber: Subscriber[_ >: JsValue])

  def props(path: String, files: List[FileInfo], bufferSize: Int) = Props(classOf[FilePublisherMultiSubscriberActor], path, files, bufferSize)
}

class FilePublisherMultiSubscriberActor(path: String, files: List[FileInfo], bufferSize: Int) extends Actor with ActorLogging {

  val buffer = context.actorOf(BufferActor.props(bufferSize))
  context.watch(buffer)
  private val watcher: ActorRef = context.actorOf(FolderWatcherActor.props(buffer, path, files))
  context.watch(watcher)

  override def receive = running(List())

  def running(subscribers: List[(ActorRef, Subscriber[_ >: JsValue])]): Receive = {

    case FilePublisherMultiSubscriberActor.Subscribe(subscriber) =>
      log.debug(s"Subscribing $subscriber")

      if (subscribers.exists(_._2 equals subscriber)) {
        subscriber.onError(new IllegalStateException(s"can not subscribe the same subscriber multiple times (see reactive-streams specification, rules 1.10 and 2.12"))
      } else {
        val subscriptionActorRef = context.actorOf(BufferSubscriptionActor.props(buffer, subscriber))
        context.watch(subscriptionActorRef)
        subscriber.onSubscribe(BufferSubscription(subscriber, subscriptionActorRef))
        context.become(running((subscriptionActorRef, subscriber) :: subscribers))
      }

    case Terminated(ref) if ref equals buffer =>
      //TODO créer un erreur dédiée.
      subscribers.foreach(_._2.onError(new IllegalStateException))

    case Terminated(ref) if ref equals watcher =>
      subscribers.foreach(s => s._1 ! BufferSubscriptionActor.Complete)

    case Terminated(ref) =>
      context.become(running(subscribers.filterNot(_._1 equals ref)))

    case _ =>
      context.become(onErrorState(List()))
      subscribers.foreach { s =>
        //TODO créer un erreur dédiée.
        s._2.onError(new IllegalStateException)
        s._1 ! PoisonPill
      }
  }

  def onErrorState(subscribers: List[(ActorRef, Subscriber[_ >: JsValue])]): Receive = {
    //TODO créer un erreur dédiée.
    case FilePublisherMultiSubscriberActor.Subscribe(subscriber) => subscriber.onError(new IllegalStateException)
    case any                                                     => log.debug(s"Unhandled message $any")
  }

}

