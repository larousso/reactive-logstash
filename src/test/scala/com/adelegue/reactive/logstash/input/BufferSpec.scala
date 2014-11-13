package com.adelegue.reactive.logstash.input

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestActorRef, TestKit, TestProbe}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import play.api.libs.json.{JsObject, Json}

import scala.concurrent.duration.DurationLong

/**
 * Test
 * Created by adelegue on 30/10/2014.
 */
class BufferSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender with WordSpecLike with Matchers with BeforeAndAfterAll {

  def this() = this(ActorSystem("BufferSpec"))

  override def afterAll() {
    TestKit.shutdownActorSystem(system)
  }

  val msg1: JsObject = Line("ligne 1", "test.txt")
  val msg2: JsObject = Line("ligne 2", "test.txt")
  val msg3: JsObject = Line("ligne 3", "test.txt")
  val msg4: JsObject = Line("ligne 4", "test.txt")
  val msg5: JsObject = Line("ligne 5", "test.txt")
  val msg6: JsObject = Line("ligne 6", "test.txt")
  val msg7: JsObject = Line("ligne 7", "test.txt")
  val msg8: JsObject = Line("ligne 8", "test.txt")
  val msg9: JsObject = Line("ligne 9", "test.txt")
  val msg10: JsObject = Line("ligne 10", "test.txt")



  "buffer actor" must {
    "add lines and give it back" in {

      val buffer = TestActorRef(BufferActor.props(10))

      buffer ! BufferActor.Entry(msg1)
      buffer ! BufferActor.Entry(msg2)

      buffer ! BufferActor.Ask(0, 2)

      expectMsg(1 second, BufferActor.Lines(List(msg1, msg2), 2, 2))

    }

    "keep buffer length" in {

      val buffer = TestActorRef(BufferActor.props(2))

      buffer ! BufferActor.Entry(msg1)
      buffer ! BufferActor.Entry(msg2)
      buffer ! BufferActor.Entry(msg3)
      buffer ! BufferActor.Entry(msg4)

      buffer ! BufferActor.Ask(2, 2)
      expectMsg(1 second, BufferActor.Lines(List(msg3, msg4), 4, 4))

      buffer ! BufferActor.Ask(0, 2)
      expectMsg(1 second, BufferActor.ErrorLinesMissing(2, 2))
    }

    "keep subscriber when lines are missing and buffer start at 0" in {

      val buffer = TestActorRef(BufferActor.props(4))

      //Ajout de 2 lignes dans le buffer
      buffer ! BufferActor.Entry(msg1)
      buffer ! BufferActor.Entry(msg2)

      //Un subscriber demande 4 ligne (il en manque deux)
      buffer ! BufferActor.Ask(0, 4)
      expectMsg(1 second, BufferActor.Lines(List(msg1, msg2), 2, 2))

      //Ajout d'une ligne supplémentaire
      buffer ! BufferActor.Entry(msg3)
      //Une ligne manquante sur deux est envoyée au subscriber
      expectMsg(1 second, BufferActor.Lines(List(msg3), 3, 3))
      //Ajout d'une ligne supplémentaire
      buffer ! BufferActor.Entry(msg4)
      //Deuxième ligne manquante sur deux est envoyée au subscriber
      expectMsg(1 second, BufferActor.Lines(List(msg4), 4, 4))

    }

    "keep subscriber when lines are missing" in {

      val buffer = TestActorRef(BufferActor.props(4))

      //Ajout de 2 lignes dans le buffer
      buffer ! BufferActor.Entry(msg1)
      buffer ! BufferActor.Entry(msg2)
      buffer ! BufferActor.Entry(msg3)
      buffer ! BufferActor.Entry(msg4)
      //Ajout de deux lignes : Le buffer est de taille 4, les 2 premières lignes sont supprimées
      buffer ! BufferActor.Entry(msg5)
      buffer ! BufferActor.Entry(msg6)

      //On demande des lignes de 0
      buffer ! BufferActor.Ask(0, 4)
      //Les lignes n'existent pas
      expectMsg(1 second, BufferActor.ErrorLinesMissing(2, 4))

      //On demande 4 lignes en partant de 4 (il en manque 2)
      buffer ! BufferActor.Ask(4, 4)
      expectMsg(1 second, BufferActor.Lines(List(msg5, msg6), 6, 6))

      buffer ! BufferActor.Entry(msg7)
      expectMsg(1 second, BufferActor.Lines(List(msg7), 7, 7))
      buffer ! BufferActor.Entry(msg8)
      expectMsg(1 second, BufferActor.Lines(List(msg8), 8, 8))

      buffer ! BufferActor.Entry(msg9)
      buffer ! BufferActor.Entry(msg10)
      expectNoMsg()
    }

    "multiple subscribers" in {

      val buffer = TestActorRef(BufferActor.props(4))
      val subsc1 = TestProbe()
      val subsc2 = TestProbe()

      //Ajout de 2 lignes dans le buffer
      buffer ! BufferActor.Entry(msg1)
      buffer ! BufferActor.Entry(msg2)
      buffer ! BufferActor.Entry(msg3)
      buffer ! BufferActor.Entry(msg4)
      //Ajout de deux lignes : Le buffer est de taille 4, les 2 premières lignes sont supprimées
      buffer ! BufferActor.Entry(msg5)
      buffer ! BufferActor.Entry(msg6)

      subsc1.send(buffer, BufferActor.Ask(4, 4))
      subsc1.expectMsg(BufferActor.Lines(List(msg5, msg6), 6, 6))

      subsc2.send(buffer, BufferActor.Ask(2, 4))
      subsc2.expectMsg(BufferActor.Lines(List(msg3, msg4, msg5, msg6), 6, 6))

      subsc2.send(buffer, BufferActor.Ask(6, 2))

      buffer ! BufferActor.Entry(msg7)
      subsc1.expectMsg(BufferActor.Lines(List(msg7), 7, 7))
      subsc2.expectMsg(BufferActor.Lines(List(msg7), 7, 7))

      buffer ! BufferActor.Entry(msg8)
      subsc1.expectMsg(BufferActor.Lines(List(msg8), 8, 8))
      subsc2.expectMsg(BufferActor.Lines(List(msg8), 8, 8))
    }

  }

}

object Line {
  def apply(msg: String, file: String) = Json.obj("message" -> msg, "file" -> file)
}
