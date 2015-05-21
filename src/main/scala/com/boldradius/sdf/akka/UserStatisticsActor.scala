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
  private var requestsPerBrowser: Map[String, Int] = Map.empty
  private var requestsPerPage: Map[String, Percent] = Map.empty
  type Hour = Int
  type Minute = Int
  private var requestsPerMinute: Map[(Hour, Minute), Int] = Map.empty.withDefaultValue(0)
  private var landingRequests: List[Request] = List.empty
  private var sinkingRequests: List[Request] = List.empty
  private var topLandingPages: Map[String, Int] = Map.empty
  private var topSinkingPages: Map[String, Int] = Map.empty
  private var topBrowsersByUser: Map[String, Int] = Map.empty.withDefaultValue(0)
  private var topReferrersByUser: Map[String, Int] = Map.empty.withDefaultValue(0)

  private var totalVisitTimePerURL: Map[String, Long] = Map.empty.withDefaultValue(0)

  val topPagesCount: Int = context.system.settings.config.getInt("akka-workshop.stats-actor.top-pages")
  val topCounts: Int = context.system.settings.config.getInt("akka-workshop.stats-actor.top-counts")

  override def receive: Receive = {
    case Requests(requests) =>
      val sortedRequests = requests.sortBy(req => req.timestamp)

      landingRequests = landingRequests :+ sortedRequests.head
      sinkingRequests = sinkingRequests :+ sortedRequests.last
      allRequests = allRequests ::: sortedRequests

      topLandingPages ++= top(topPagesCount, landingRequests, UserStatisticsActor.groupByUrl, UserStatisticsActor.mapToCount)
      topSinkingPages ++= top(topPagesCount, sinkingRequests, UserStatisticsActor.groupByUrl, UserStatisticsActor.mapToCount)


      def accumulateMapCount(oldMap: Map[String, Int], newMap: Map[String, Int]): Map[String, Int] = {
        newMap map {
          case (key, value) => key -> (oldMap(key) + value)
        }
      }

      topBrowsersByUser ++= accumulateMapCount(topBrowsersByUser,
        top(topCounts, sortedRequests, UserStatisticsActor.groupByBrowser, UserStatisticsActor.mapToUserCount))

      topReferrersByUser ++= accumulateMapCount(topReferrersByUser,
        top(topCounts, sortedRequests, UserStatisticsActor.groupByReferrer, UserStatisticsActor.mapToUserCount))

      requestsPerBrowser ++= accumulateMapCount(requestsPerBrowser,
        all(sortedRequests, UserStatisticsActor.groupByBrowser, UserStatisticsActor.mapToCount))

      pageVisitsAggregation(allRequests)

      timeAggregation(sortedRequests)
  }

  def generateStats: String = {
    val reqPerBrowser = s"Number of requests per browser:\n${requestsPerBrowser.toList.mkString("\n")}"
    val ((busyHour, busyMin), countReqs) = requestsPerMinute.maxBy(tuple => tuple._2)
    val busyTime = s"Busiest time of the day: $busyHour:$busyMin with #$countReqs requests"
    val reqPerPage = s"Page visit distribution:\n${requestsPerPage.toList.mkString("\n")}"
    val visitTimePerPage = s"Total visit time per page:\n${totalVisitTimePerURL.mkString("\n")}"
    ""
  }

  /**
   * The top pages from the passed requests.
   * @return Map of page URL to Hits
   */
  def top(number: Int, requests: List[Request],
               groupBy: (Request) => String,
               mapTo: ((String, List[Request])) => (String, Int)): Map[String, Int] = {
    @tailrec
    def getMax(workingMap: Map[String, Int], returnMap: Map[String, Int] = Map.empty): Map[String, Int] = workingMap match {
      case map if workingMap.isEmpty => returnMap
      case map if returnMap.size == number => returnMap
      case map =>
        val currentMax @ (maxUrl, _) = map.maxBy { case (_, size) => size }
        getMax(map - maxUrl, returnMap + currentMax)
    }

    getMax(all(requests, groupBy, mapTo))
  }

  def all(requests: List[Request],
          groupBy: (Request) => String,
          mapTo: ((String, List[Request])) => (String, Int)): Map[String, Int] = {
    requests.groupBy(groupBy).map(mapTo)
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

  val groupByUrl: Request => String = req => req.url
  val groupByBrowser: Request => String = req => req.browser
  val groupByReferrer: Request => String = req => req.referrer

  val mapToCount: ((String, List[Request])) => (String, Int) = {
    case (name, reqs) => name -> reqs.size
  }
  val mapToUserCount: ((String, List[Request])) => (String, Int) = {
    case (browser, reqs) => browser -> reqs.groupBy(_.sessionId).size
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
}
