package com.boldradius.sdf.akka

import akka.actor.{Props, Actor, ActorLogging}
import com.boldradius.sdf.akka.SessionHandlingActor.Requests
import com.boldradius.sdf.akka.UserStatisticsActor.Percent
import org.joda.time.DateTime

import scala.annotation.tailrec

//case class Request(sessionId: Long, timestamp: Long, url: String, referrer: String, browser: String)

class UserStatisticsActor extends Actor with ActorLogging {

  private var allRequests: List[Request] = List()

  // Aggregations
  type UserId = Long
  private var requestsPerBrowser: Map[String, Map[UserId, Int]] = Map.empty.withDefaultValue(Map.empty)
  private var requestsPerReferrer: Map[String, Map[UserId, Int]] = Map.empty.withDefaultValue(Map.empty)
  private var requestsPerPage: Map[String, Percent] = Map.empty
  type Hour = Int
  type Minute = Int
  private var requestsPerMinute: Map[(Hour, Minute), Int] = Map.empty.withDefaultValue(0)
  private var landingRequests: List[Request] = List.empty
  private var sinkingRequests: List[Request] = List.empty
  private var topLandingPages: Map[String, Int] = Map.empty
  private var topSinkingPages: Map[String, Int] = Map.empty

  private var totalVisitTimePerURL: Map[String, Long] = Map.empty.withDefaultValue(0)

  val topPagesCount: Int = context.system.settings.config.getInt("akka-workshop.stats-actor.top-pages")

  override def receive: Receive = {
    case Requests(requests) =>
      val sortedRequests = requests.sortBy(req => req.timestamp)
      landingRequests = landingRequests :+ sortedRequests.head
      sinkingRequests = sinkingRequests :+ sortedRequests.last
      allRequests = allRequests ::: requests

      topLandingPages ++= topPages(topPagesCount, landingRequests)
      topSinkingPages ++= topPages(topPagesCount, sinkingRequests)

      browserAggregation(requests)
      referrerAggregation(requests)


      pageVisitsAggregation(allRequests)
      timeAggregation(requests)
  }

  def generateStats: String = {
    val reqPerBrowser = s"Number of requests per browser:\n${requestsPerBrowserAggregation.toList.mkString("\n")}"
    val ((busyHour, busyMin), countReqs) = requestsPerMinute.maxBy(tuple => tuple._2)
    val busyTime = s"Busiest time of the day: $busyHour:$busyMin with #$countReqs requests"
    val reqPerPage = s"Page visit distribution:\n${requestsPerPage.toList.mkString("\n")}"
    val visitTimePerPage = s"Total visit time per page:\n${totalVisitTimePerURL.mkString("\n")}"
    ""
  }

  def browserAggregation(requests: List[Request]): Unit = {
    val newReqPerBrowsers = browserOrReferrerAggregation(requests, req => req.browser, requestsPerBrowser)
    requestsPerBrowser = requestsPerBrowser ++ newReqPerBrowsers
  }

  def referrerAggregation(requests: List[Request]): Unit = {
    val newReferPerBrowser = browserOrReferrerAggregation(requests, req => req.referrer, requestsPerReferrer)
    requestsPerReferrer = requestsPerReferrer ++ newReferPerBrowser
  }

  def browserOrReferrerAggregation(requests: List[Request], groupByFunc: Request => String, oldMapping: Map[String, Map[UserId, Int]]): Map[String, Map[UserId, Int]] = {
    requests.groupBy(groupByFunc).map { case (key, reqs) =>
      val newMap: Map[UserId, List[Request]] = reqs.groupBy(_.sessionId)
      val oldMap: Map[UserId, Int] = oldMapping(key)

      // combine 2 mapping to the new one
      val allUserId: Set[UserId] = oldMap.keySet ++ newMap.keySet
      val combineMap: Map[UserId, Int] = allUserId.map(userId => {
        val newCount = newMap.getOrElse(userId, List.empty).size
        val oldCount = oldMap.getOrElse(userId, 0)
        userId -> (oldCount + newCount)
      }).toMap

      key -> combineMap
    }
  }

  /**
   * The top pages by hits from the passed requests.
   * @return Map of page URL to Hits
   */
  def topPages(number: Int, requests: List[Request]): Map[String, Int] = {
    @tailrec
    def getMax(workingMap: Map[String, Int], returnMap: Map[String, Int] = Map.empty): Map[String, Int] = workingMap match {
      case map if workingMap.isEmpty => returnMap
      case map if returnMap.size == number => returnMap
      case map =>
        val currentMax @ (maxUrl, _) = map.maxBy { case (_, size) => size }
        getMax(map - maxUrl, returnMap + currentMax)
    }

    val pagesByHits = requests.groupBy(req => req.url).map {
      case (url, reqs) => url -> reqs.size
    }
    getMax(pagesByHits)
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
  def getTopBrowser(count: Int): List[(String, Int)] = {
    val usersPerBrowser: List[(String, Int)] = usersPerBrowserAggregation.toList
    val greaterThan = (tuple1: (String, Int), tuple2: (String, Int)) => tuple1._2 > tuple2._2
    getMultiMax(usersPerBrowser, count, greaterThan)
  }

  // complexity O(count * size(list))
  def getMultiMax[A](list: List[A], count: Int, greaterThan: (A, A) => Boolean): List[A] = {

    def addToSortedList(elem: A, descendingList: List[A]): List[A] = descendingList match {
      case head :: tail =>
        if (greaterThan(elem, head)) elem :: head :: tail
        else head :: addToSortedList(elem, tail)
      case Nil => List(elem)
    }

    list.foldLeft(List.empty[A])((result, elem) => {
      val newResult = addToSortedList(elem, result)
      if (newResult.size > count)
        newResult.init
      else
        newResult
    })
  }

  // Find top 2 referrer
  def getTopReferrer(count: Int): List[(String, Int)] = {
    val usersPerReferrer: List[(String, Int)] = requestsPerReferrer.toList.map {
      case (referrer, requestsPerUser) =>
        referrer -> requestsPerUser.count(tuple => tuple._2 > 0)
    }

    val greaterThan = (tuple1: (String, Int), tuple2: (String, Int)) => tuple1._2 > tuple2._2
    getMultiMax(usersPerReferrer, count, greaterThan)
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
