package com.boldradius.sdf.akka

import akka.actor.{Props, Actor, ActorLogging}
import org.joda.time.DateTime

//case class Request(sessionId: Long, timestamp: Long, url: String, referrer: String, browser: String)

class UserStatisticsActor extends Actor with ActorLogging {

  private var allRequests: List[Request] = List()

  // Aggregations
  private var requestsPerBrowser: Map[String, Int] = Map().withDefaultValue(0)
  type Hour = Int
  type Minute = Int
  private var requestsPerMinute: Map[(Hour, Minute), Int] = Map.empty.withDefaultValue(0)

  private var totalVisitTimePerURL: Map[String, Long] = Map.empty.withDefaultValue(0)

  override def receive: Receive = {
    case SessionHandlingActor.Requests(requests) =>
      allRequests = allRequests ::: requests
      browserUsersAggregation(requests)
      timeAggregation(requests)
  }

  // Number of requests per browser
  def browserUsersAggregation(requests: List[Request]): Map[String, Int] = {
    val newBrowserAggregation = requests.groupBy(_.browser).map { case (browser, reqs) =>
      browser -> reqs.size
    }

    newBrowserAggregation.foreach { case (browser, count) =>
        val oldCount = requestsPerBrowser(browser)
        requestsPerBrowser += browser -> (oldCount + count)
    }
    requestsPerBrowser
  }

  // Number of requests per minute of the day
  def timeAggregation(requests: List[Request]): Map[(Hour, Minute), Int] = {
    val newTimeAggregation: Map[(Hour, Minute), Int] =
      requests.groupBy(request => {
        val date = new DateTime(request.timestamp)
        (date.getHourOfDay, date.getMinuteOfHour)
      }).map {
        case ((hour, time), reqs) => (hour, time) -> reqs.size
      }

    newTimeAggregation.foreach { case (time, count) =>
      val oldCount = requestsPerMinute(time)
      requestsPerMinute += time -> (oldCount + count)
    }
    requestsPerMinute
  }

  // Average visit time per URL
  def visitTimePerURLAggregation(requests: List[Request]): Map[String, Long] = {
    val sortedRequests = requests.sortBy(_.timestamp)
    val consecutivePairOfRequests: List[(Request, Request)] = sortedRequests zip sortedRequests.tail

    val visitTimePerURL: List[(String, Long)] = consecutivePairOfRequests map {
      case (req1, req2) =>
        req1.url -> (req2.timestamp - req1.timestamp)
    }

    val totalVisitTime: Map[String, Long] = visitTimePerURL.groupBy(_._1).map {
      case (url, list: List[(String, Long)]) => url -> list.map(_._2).sum
    }

    totalVisitTime.foreach { case (url, time) =>
      val oldTime = totalVisitTimePerURL(url)
      totalVisitTimePerURL += url -> (oldTime + time)
    }

    totalVisitTimePerURL
  }

}

object UserStatisticsActor {
  def props: Props = Props[UserStatisticsActor]
}
