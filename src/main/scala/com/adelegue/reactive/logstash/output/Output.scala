package com.adelegue.reactive.logstash.output

import play.api.libs.json.JsValue

import scala.concurrent.Future

/**
 * Created by adelegue on 14/11/2014.
 */
trait Output {

  def apply(message: JsValue): Future[Unit]

}
