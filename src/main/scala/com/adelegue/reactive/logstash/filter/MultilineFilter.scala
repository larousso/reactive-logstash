package com.adelegue.reactive.logstash.filter

import akka.stream.Transformer
import play.api.libs.json.{JsObject, Json, JsValue}

import scala.collection.immutable.Seq
import scala.util.matching.Regex

/**
 * Created by adelegue on 25/11/14.
 */

object MultilineFilter {
  def apply(pattern: String, negate: Boolean = false, what: What = Next): () => Transformer[JsValue, JsValue] = {
    () => new MultilineFilter(pattern, negate, what)
  }
}

sealed trait What
case object Previous extends What
case object Next extends What


class MultilineFilter(pattern: String, negate: Boolean, what: What) extends Transformer[JsValue, JsValue]{

  var previousJson = Json.obj()
  var concat = false

  override def onNext(element: JsValue): Seq[JsValue] = {
    val line = getLine(element)
    if(line.matches(pattern) != negate){
      val newLine = getLine(previousJson) + "\n" + line
      previousJson = previousJson.as[JsObject] ++ Json.obj("message" -> newLine)
      concat = true
      Seq()
    } else {
      val values: Seq[JsValue] =
        if(previousJson.values.isEmpty){
          Seq(element)
        } else {
          if(concat) {
            concat = false
            Seq(previousJson, element)
          } else {
            Seq(element)
          }
        }
      previousJson = element.as[JsObject]
      values
    }
  }

  def getLine(json: JsValue) = (json \ "message").as[String]
}
