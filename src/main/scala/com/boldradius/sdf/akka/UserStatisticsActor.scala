package com.boldradius.sdf.akka

import akka.actor.{Props, Actor, ActorLogging}
import com.boldradius.sdf.akka.SessionHandlingActor.Requests
import com.boldradius.sdf.akka.UserStatisticsActor.Percent
import org.joda.time.DateTime

//case class Request(sessionId: Long, timestamp: Long, url: String, referrer: String, browser: String)

class UserStatisticsActor extends Actor with ActorLogging {

  private var allRequests: List[Request] = List()

  // Aggregations
  private var requestsPerBrowser: Map[String, Int] = Map().withDefaultValue(0)
  private var requestsPerPage: Map[String, Percent] = Map.empty
  type Hour = Int
  type Minute = Int
  private var requestsPerMinute: Map[(Hour, Minute), Int] = Map.empty.withDefaultValue(0)
  private var landingRequests: List[Request] = List.empty
  private var sinkingRequests: List[Request] = List.empty
  private var topThreeLandingPages: Map[String, Int] = Map.empty
  private var topThreeSinkingPages: Map[String, Int] = Map.empty

  override def receive: Receive = {
    case Requests(requests) =>
      val sortedRequests = requests.sortBy(req => req.timestamp)
      landingRequests = landingRequests :+ sortedRequests.head
      sinkingRequests = sinkingRequests :+ sortedRequests.last
      allRequests = allRequests ::: requests

      topThreeLandingPages ++= topThreePages(landingRequests)
      topThreeSinkingPages ++= topThreePages(sinkingRequests)
      browserUsersAggregation(requests)
      pageVisitsAggregation(allRequests)
      timeAggregation(requests)
  }

  /**
   * The top three pages by hits from the passed requests.
   * @param requests
   * @return Map of page URL to Hits
   */
  def topThreePages(requests: List[Request]): Map[String, Int] = {
    val pagesByHits = requests.groupBy(req => req.url).map {
      case (url, reqs) => url -> reqs.size
    }
    val sortedByHits = pagesByHits.toSeq.sortBy { case (url, size) => size }
    sortedByHits.takeRight(3).toMap
  }

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

  def pageVisitsAggregation(requests: List[Request]): Map[String, Percent] = {
    val totalCount = requests.size.toDouble
    requestsPerPage ++= requests.groupBy(_.url).map { case (url, reqs) =>
      url -> Percent(reqs.size / totalCount * 100)
    }
    requestsPerPage
  }

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

}

object UserStatisticsActor {
  def props: Props = Props[UserStatisticsActor]

  case class Percent(percent: Double) {
    override def toString = f"$percent%.2f"
  }
}
