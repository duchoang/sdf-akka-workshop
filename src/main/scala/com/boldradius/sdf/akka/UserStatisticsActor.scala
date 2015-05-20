package com.boldradius.sdf.akka

import akka.actor.{Props, Actor, ActorLogging}
import com.boldradius.sdf.akka.SessionHandlingActor.Requests
import com.boldradius.sdf.akka.UserStatisticsActor.Percent
import org.joda.time.DateTime

//case class Request(sessionId: Long, timestamp: Long, url: String, referrer: String, browser: String)

class UserStatisticsActor extends Actor with ActorLogging {

  private var allRequests: List[Request] = List()

  // Aggregations
  type UserId = Long
  private var requestsPerBrowser: Map[String, Map[UserId, Int]] = Map.empty.withDefaultValue(Map.empty)
  private var requestsPerPage: Map[String, Percent] = Map.empty
  type Hour = Int
  type Minute = Int
  private var requestsPerMinute: Map[(Hour, Minute), Int] = Map.empty.withDefaultValue(0)
  private var landingRequests: List[Request] = List.empty
  private var sinkingRequests: List[Request] = List.empty
  private var topThreeLandingPages: Map[String, Int] = Map.empty
  private var topThreeSinkingPages: Map[String, Int] = Map.empty

  private var totalVisitTimePerURL: Map[String, Long] = Map.empty.withDefaultValue(0)

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


  def browserUsersAggregation(requests: List[Request]): Unit = {
    requests.groupBy(_.browser).foreach { case (browser, reqs) =>
      val newMap: Map[UserId, List[Request]] = reqs.groupBy(_.sessionId)
      val oldMap: Map[UserId, Int] = requestsPerBrowser(browser)

      // combine 2 mapping to the new one
      val allUserId: Set[UserId] = oldMap.keySet ++ newMap.keySet
      val combineMap: Map[UserId, Int] = allUserId.map(userId => {
        val newCount = newMap.getOrElse(userId, List.empty).size
        val oldCount = oldMap.getOrElse(userId, 0)
        userId -> (oldCount + newCount)
      }).toMap

      requestsPerBrowser += browser -> combineMap
    }
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

  // Number of requests per browser
  def requestsPerBrowserAggregation: Map[String, Int] = {
    requestsPerBrowser.map {
      case (browser, requestsPerUser) =>
        browser -> requestsPerUser.map(_._2).sum
    }
  }

  // Number of users per browser
  def usersPerBrowserAggregation: Map[String, Int] = {
    requestsPerBrowser.map {
      case (browser, requestsPerUser) =>
        browser -> requestsPerUser.count(tuple => tuple._2 > 0)
    }
  }

  // Find top 2 browser
  def getTopBrowser: (Option[(String, Int)], Option[(String, Int)]) = {
    val usersPerBrowser: Map[String, Int] = usersPerBrowserAggregation
    val sorted = usersPerBrowser.toList.sortBy(tuple => tuple._2)
    (sorted.lastOption, sorted.init.lastOption)
  }

  // Page visit distribution
  def pageVisitsAggregation(requests: List[Request]): Map[String, Percent] = {
    val totalCount = requests.size.toDouble
    requestsPerPage ++= requests.groupBy(_.url).map { case (url, reqs) =>
      url -> Percent(reqs.size / totalCount * 100)
    }
    requestsPerPage
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

  case class Percent(percent: Double) {
    override def toString = f"$percent%.2f"
  }
}
