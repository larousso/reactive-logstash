package com.adelegue.reactive.logstash.input.impl

import akka.actor.{ Actor, ActorLogging, ActorRef, Props }
import com.adelegue.reactive.logstash.input.impl.BufferActor._
import play.api.libs.json.JsValue

object BufferActor {
  case class Entry(value: JsValue)
  case class Ask(from: Long, to: Long)
  case class Lines(list: List[JsValue], position: Long, bufferSize: Long)
  case class ErrorLinesMissing(start: Long, size: Long) extends Throwable
  case class WaitingSubscription(subscription: ActorRef, nbMissingLines: Long, currentPosition: Long)
  case object AskSize
  case class Size(size: Long)

  def props(bufferSize: Int) = Props(classOf[BufferActor], bufferSize)

}

class BufferActor(bufferSize: Int) extends Actor with ActorLogging {

  override def receive = buffer(List(), 0, List())

  def buffer(currentBuffer: List[JsValue], start: Long, waitingList: List[BufferActor.WaitingSubscription]): Actor.Receive = {

    //Nouvelle entree
    case Entry(elt) if currentBuffer.size < bufferSize =>
      //log.debug(s"One entry added to buffer $elt")
      val newBuffer: List[JsValue] = elt :: currentBuffer
      //log.debug(s"NEW BUFFER ---- [ ${newBuffer.map(_.line).mkString(", ")} ]")
      context.become(buffer(newBuffer, start, handleWaitingSubscribers(elt, waitingList, start + newBuffer.size)))

    //Nouvelle entrée avec réajustement du buffer
    case Entry(elt) if currentBuffer.size >= bufferSize =>
      val newBuffer = elt :: currentBuffer.reverse.drop(1).reverse
      //log.debug(s"One entry added to buffer $elt, one entry removed from buffer")
      val newStart: Long = start + 1
      //log.debug(s"NEW BUFFER ---- [ ${newBuffer.map(_.line).mkString(", ")} ], start = $newStart")
      context.become(buffer(newBuffer, newStart, handleWaitingSubscribers(elt, waitingList, newStart + newBuffer.size)))

    case Ask(from, length) if currentBuffer.size == 0 =>
      //log.debug(s"Empty buffer ${sender()} added to the waiting list")
      val newWaitingList = addToWaitingList(sender(), length, 0, waitingList)
      context.become(buffer(currentBuffer, start, newWaitingList))

    case Ask(from, length) if from < start =>
      log.error(s"missing entries from buffer. asked from $from but available from $start")
      sender() ! BufferActor.ErrorLinesMissing(start, currentBuffer.size)

    //    case Ask(from, length) if from >= start + currentBuffer.size =>
    //      //TODO ajout du subsc à la file d'attente, les lignes n'existent pas encore
    //      val currentPosition: Long = from + currentBuffer.size
    //      val newWaitingList = addToWaitingList(sender(), length, from, waitingList)
    //      context.become(buffer(currentBuffer, start, newWaitingList))

    case Ask(from, length) if (from >= start) && (from <= start + currentBuffer.size) =>
      log.debug(s"${sender()} ask $length entries from $from, current buffer start at $start to ${start + currentBuffer.size} ")
      val tmpBuffer: List[JsValue] = currentBuffer.reverse.drop((from - start).toInt)
      val elementsToSend: List[JsValue] = tmpBuffer.take(adaptLengthToMaxBufferSize(length, tmpBuffer.length))
      val elementsToSendSize = elementsToSend.size
      val currentPosition: Long = from + elementsToSendSize
      if (elementsToSendSize < length) {
        val nbEltsMissing = length - elementsToSendSize
        log.debug(s"entries are missing, ${sender()} is added to waiting lines, missing $nbEltsMissing elts")
        val newWaitingList = addToWaitingList(sender(), nbEltsMissing, currentPosition, waitingList)
        context.become(buffer(currentBuffer, start, newWaitingList))
      }
      if (elementsToSendSize > 0) {
        log.debug(s"Sending $elementsToSendSize elements to ${sender()} with position $currentPosition")
        sender() ! BufferActor.Lines(elementsToSend, currentPosition, start + currentBuffer.size)
      }

    case AskSize => sender() ! Size(start + currentBuffer.size)

    case msg     => log.debug(s"[Buffer] - Unhandled message $msg")
  }

  def adaptLengthToMaxBufferSize(length: Long, bufferSize: Int): Int = {
    length match {
      case l if l > bufferSize => bufferSize
      case l                   => l.toInt
    }
  }

  def addToWaitingList(subscription: ActorRef, nbEltsMissing: Long, currentPosition: Long, waitingList: List[WaitingSubscription]): List[WaitingSubscription] = {
    waitingList.find(_.subscription equals subscription) match {

      case Some(existingSubscription) => waitingList.map {
        case elt if elt.subscription equals subscription =>
          BufferActor.WaitingSubscription(subscription, nbEltsMissing + elt.nbMissingLines, currentPosition)

        case elt => elt
      }

      case None => BufferActor.WaitingSubscription(sender(), nbEltsMissing, currentPosition) :: waitingList
    }
  }

  def handleWaitingSubscribers(elt: JsValue, waitingSubscribers: List[BufferActor.WaitingSubscription], totalSize: Long): List[BufferActor.WaitingSubscription] = {
    waitingSubscribers
      .map { waitingSubscriber =>
        waitingSubscriber.subscription ! BufferActor.Lines(List(elt), waitingSubscriber.currentPosition + 1, totalSize)
        waitingSubscriber.copy(nbMissingLines = waitingSubscriber.nbMissingLines - 1, currentPosition = waitingSubscriber.currentPosition + 1)
      }
      .filterNot(_.nbMissingLines == 0)
  }

}