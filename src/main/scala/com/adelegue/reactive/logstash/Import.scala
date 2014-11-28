package com.adelegue.reactive.logstash

import akka.actor.ActorSystem
import akka.stream.FlowMaterializer

/**
 * Created by adelegue on 07/10/2014.
 */
object Import {
    implicit val system = ActorSystem("ReactiveLogstash")
    implicit val materializer = FlowMaterializer()
    implicit val ec = system.dispatcher

}
