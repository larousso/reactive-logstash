package com.adelegue.reactive.logstash.codec

import org.joda.time.DateTime
import play.api.libs.json.{JsValue, Json}

/**
 * Created by adelegue on 16/11/14.
 */
object PlainCodec extends Codec[String, JsValue] {
  override def apply(any: String): JsValue = Json.obj("@timestamp" -> DateTime.now().getMillis, "message" -> any)
}

object DoNothingCodec extends Codec[JsValue, JsValue] {
  override def apply(any: JsValue): JsValue = any
}