package com.adelegue.reactive.logstash.codec

import play.api.libs.json.JsValue

/**
 * Created by adelegue on 14/11/2014.
 */
trait Codec[T] {

  def apply(any: T): JsValue

}
