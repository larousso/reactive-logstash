package com.adelegue.reactive.logstash

import akka.actor.ActorSystem

/**
 * Created by adelegue on 07/10/2014.
 */
object App {

  def main(args: Array[String]): Unit = {
    implicit val system = ActorSystem("Sys")

  }

}
