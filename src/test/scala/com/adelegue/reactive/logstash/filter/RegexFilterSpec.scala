package com.adelegue.reactive.logstash.filter

import demo.Indexer
import org.scalatest.{Matchers, FlatSpec}

import scala.util.matching.Regex

/**
 * Created by adelegue on 22/12/14.
 */
class RegexFilterSpec  extends FlatSpec with Matchers {

  val YEAR = "([\\+-]?\\d{4}(?!\\d{2}\\b))"
  val ISO_DATE_REGEX = s"$YEAR((-?)((0[1-9]|1[0-2])(\\3([12]\\d|0[1-9]|3[01]))?|W([0-4]\\d|5[0-2])(-?[1-7])?|(00[1-9]|0[1-9]\\d|[12]\\d{2}|3([0-5]\\d|6[1-6])))([T\\s]((([01]\\d|2[0-3])((:?)[0-5]\\d)?|24\\:?00)([\\.,]\\d+(?!:))?)?(\\17[0-5]\\d([\\.,]\\d+)?)?([zZ]|([\\+-])([01]\\d|2[0-3]):?([0-5]\\d)?)?)?)?"
  val DATE_ISO = s"(\\d{4}\\-\\d{2}\\-\\d{2} \\d{2}:\\d{2}:\\d{2},\\d{3})"
  val LOG_LEVEL = "(DEBUG|INFO|WARNING|ERROR)"
  val WORD = "(\\w+)"
  val NUMBER = "(\\d+\\.\\d+)"
  val INTEGER = "(\\d+)"
  val DATE = "(\\d{2}\\-\\d{2}\\-\\d{4})"
  val NOTSPACE = "(\\S+)"
  val DATA = "(.*)"
  val pattern = s"$DATE_ISO \\- \\[$LOG_LEVEL\\] \\- from $WORD in $WORD message numéro $NUMBER current time $DATE, bois : $NOTSPACE, coord : \\[$NUMBER, $NUMBER\\] $DATA".r
  val pattern2 = s"^$DATE_ISO \\- \\[$LOG_LEVEL\\] \\- from $WORD in $NOTSPACE message numéro $INTEGER current time $DATE, bois : $NOTSPACE, coord : \\[$NUMBER, $NUMBER\\] $DATA$$".r


  val line = "2014-12-22 13:12:51,125 - [DEBUG] - from file in RxComputationThreadPool-2 message numéro 17 current time 22-12-2014, bois : Cerisier, coord : [47.830333, 1.489276] "


  "Line" should "match regex" in {

    println(pattern2)

    line match {
      case pattern(timestamp, loglevel, appender, thread, messageNum, messageDate, wood, lat, long, message) =>
        println("1")
      case pattern2(timestamp, loglevel, appender, thread, messageNum, messageDate, wood, lat, long, message) =>
        println("2")
      case _ =>
        fail()
    }
  }

}



