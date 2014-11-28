package demo

import akka.stream.scaladsl.{Source, SubscriberSink}
import com.adelegue.reactive.logstash.Import._
import com.adelegue.reactive.logstash.input.publisher.FilePublisher
import com.adelegue.reactive.logstash.output.RedisOutput

/**
 * Created by adelegue on 28/11/14.
 */
object Agent {

  def main (args: Array[String]) {
    Source(FilePublisher("/Users/adelegue/Documents/conf/breizhcamp/demo/logproducer/logs", List("wood.log")))
      .map{l => println(s"GOING TO PUBLISH TO REDIS $l"); l}
      .runWith(SubscriberSink(RedisOutput()))
  }

}
