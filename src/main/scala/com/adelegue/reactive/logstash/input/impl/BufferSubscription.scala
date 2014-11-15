package com.adelegue.reactive.logstash.input.impl

import akka.actor._
import org.reactivestreams.{ Subscriber, Subscription }
import play.api.libs.json.JsValue

object BufferSubscription {
  def apply(subscriber: Subscriber[_ >: JsValue], subscriptionActorRef: ActorRef) = new BufferSubscription(subscriber, subscriptionActorRef)
}

class BufferSubscription(subscriber: Subscriber[_ >: JsValue], subscriptionActorRef: ActorRef) extends Subscription {
  val maxRequestedElements: Long = java.lang.Long.MAX_VALUE

  override def cancel(): Unit = subscriptionActorRef ! BufferSubscriptionActor.Cancel
  override def request(nbElts: Long): Unit = {
    if (nbElts > maxRequestedElements) {
      throw new IllegalArgumentException(s"The maximum number of elements requested is $maxRequestedElements")
    } else if (nbElts > 0) {
      subscriptionActorRef ! BufferSubscriptionActor.Request(nbElts)
    } else {
      throw new IllegalArgumentException("The number of elements requested must be >= 0, rule 3.9")
    }
  }
}

object BufferSubscriptionActor {
  case class Request(nbElements: Long)
  case object Complete
  case object Cancel

  def props(buffer: ActorRef, subscriber: Subscriber[_ >: JsValue]) = Props(classOf[BufferSubscriptionActor], buffer, subscriber)

}

class BufferSubscriptionActor(buffer: ActorRef, subscriber: Subscriber[JsValue]) extends Actor with ActorLogging {

  val messageMaxRequestReached: String = "The max number of element requested (2^63) is reached"
  val stateRunning = "WaitingRequest"
  val stateWaiting = "WaitingBuffer"
  val stateComplete = "Prepare complete"

  override def receive = waitingRequest(0)

  def waitingRequest(currentPosition: Long): Actor.Receive = {

    case BufferSubscriptionActor.Request(nbElt) if nbElt > java.lang.Long.MAX_VALUE =>
      onError(subscriber, new IllegalStateException(messageMaxRequestReached))

    case BufferSubscriptionActor.Request(nbElt) =>
      log.debug(s"[$stateRunning] - Asking $nbElt line from $currentPosition. Current position $currentPosition")
      buffer ! BufferActor.Ask(currentPosition, nbElt)
      context.become(waitingElementsFromBuffer(currentPosition, nbElt))

    case BufferSubscriptionActor.Complete =>
      log.debug(s"[$stateRunning] - No more files preparing to close the streams. Current position $currentPosition")
      context.become(prepareOnComplete(currentPosition, 0))
      buffer ! BufferActor.AskSize

    case BufferSubscriptionActor.Cancel =>
      context.system.stop(self)
  }

  def waitingElementsFromBuffer(currentPosition: Long, elementsMissing: Long): Actor.Receive = {

    case any if handleLines(currentPosition, elementsMissing, stateWaiting).isDefinedAt(any) =>
      handleLines(currentPosition, elementsMissing, stateWaiting).apply(any)

    case BufferSubscriptionActor.Request(nbElt) if (elementsMissing + nbElt) > java.lang.Long.MAX_VALUE =>
      onError(subscriber, new IllegalStateException(messageMaxRequestReached))

    case BufferSubscriptionActor.Request(nbElt) =>
      log.debug(s"[Waiting] - Asking $nbElt line. Current position $currentPosition, currentNbRequested : $elementsMissing")

      buffer ! BufferActor.Ask(currentPosition + elementsMissing, nbElt)
      context.become(waitingElementsFromBuffer(currentPosition, nbElt + elementsMissing))

    case BufferSubscriptionActor.Complete =>
      //Attendre la fin des messages
      log.debug(s"[$stateWaiting] - No more files preparing to close the streams. Current position $currentPosition, currentNbRequested : $elementsMissing")
      context.become(prepareOnComplete(currentPosition, elementsMissing))
      buffer ! BufferActor.AskSize

    case any => log.error(s"[$stateWaiting] - Unexpected message $any.")
  }

  /*
    Prepare onComplete state:
    - wait for lines comming from buffer if missing
    -
   */
  def prepareOnComplete(currentPosition: Long, currentNbRequested: Long): Receive = {

    //All lines have been processed, processing onComplete on subscriber
    case BufferActor.Lines(list, position, size) if size equals position =>
      onComplete(subscriber)

    //Receiving current size of the buffer
    case BufferActor.Size(size) if (currentPosition > 0) && (currentPosition equals size) =>
      onComplete(subscriber)

    case BufferSubscriptionActor.Request(nbElt) if (currentNbRequested + nbElt) > java.lang.Long.MAX_VALUE =>
      onError(subscriber, new IllegalStateException(messageMaxRequestReached))

    //Current position == 0 while on complete state mean that stream is closed before any request arrived.
    //Request are processed and the onComplete is called when it's finished
    case BufferSubscriptionActor.Request(nbElt) if currentPosition == 0 =>
      buffer ! BufferActor.Ask(0, nbElt)
      context.become(prepareOnComplete(currentPosition, currentNbRequested + nbElt))

    case any if handleLines(currentPosition, currentNbRequested, stateComplete).isDefinedAt(any) =>
      handleLines(currentPosition, currentNbRequested, stateComplete).apply(any)

    case any =>
      log.error(s"[$stateComplete] - Message $any not handled while waiting to complete stream. Current position $currentPosition, currentNbRequested : $currentNbRequested")
  }

  /*
    Handle lines arriving from buffer
   */
  def handleLines(currentPosition: Long, currentNbRequested: Long, state: String): Receive = {

    case BufferActor.Lines(list, position, size) if position < currentPosition + currentNbRequested =>
      log.debug(s"[$state] - Receiving line $list, Buffer[position: $position, size: $size], Current position $currentPosition, currentNbRequested : $currentNbRequested")
      list.foreach { l =>
        subscriber.onNext(l)
      }
      context.become(waitingElementsFromBuffer(position, currentNbRequested - list.length))

    case BufferActor.Lines(list, position, size) if position >= currentPosition + currentNbRequested =>
      log.debug(s"[$state] - Receiving line $list, Buffer[position: $position, size: $size], Current position $currentPosition, currentNbRequested : $currentNbRequested")
      list.foreach { l =>
        subscriber.onNext(l)
      }
      context.become(waitingRequest(position))

    case err @ BufferActor.ErrorLinesMissing(from, size) =>
      onError(subscriber, new IllegalStateException(s"[$state] - Error lines are missing from buffer, Current position $currentPosition, currentNbRequested : $currentNbRequested"))
  }

  def onError(subscriber: Subscriber[JsValue], exception: Exception) = {
    log.error(s"Error on subscription $self", exception)
    subscriber.onError(exception)
    self ! PoisonPill
  }

  def onComplete(subscriber: Subscriber[JsValue]) = {
    log.debug(s"calling onComplete on subscriber $subscriber")
    subscriber.onComplete()
    self ! PoisonPill
  }

}
