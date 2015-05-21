package com.boldradius.sdf.akka

import akka.actor.{Props, Actor, ActorLogging}
import com.boldradius.sdf.akka.SessionHandlingActor.Requests
import com.boldradius.sdf.akka.UserStatisticsActor.Percent
import org.joda.time.DateTime

import scala.annotation.tailrec

//case class Request(sessionId: Long, timestamp: Long, url: String, referrer: String, browser: String)

class UserStatisticsActor extends Actor with ActorLogging {

  import UserStatisticsActor._

  private var allRequests: List[Request] = List()

  // Aggregations
  private var requestsPerBrowser: Map[String, Int] = Map.empty.withDefaultValue(0)
  private var requestsPerPage: Map[String, Int] = Map.empty.withDefaultValue(0)
  private var percentPerPage: Map[String, Percent] = Map.empty
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
      throw new IllegalArgumentException("requests")
      val sortedRequests = requests.sortBy(req => req.timestamp)

      landingRequests = landingRequests :+ sortedRequests.head
      sinkingRequests = sinkingRequests :+ sortedRequests.last
      allRequests = allRequests ::: sortedRequests

      topLandingPages ++= top(topPagesCount, landingRequests, UserStatisticsActor.groupByUrl, UserStatisticsActor.mapToCount)
      topSinkingPages ++= top(topPagesCount, sinkingRequests, UserStatisticsActor.groupByUrl, UserStatisticsActor.mapToCount)


      def accumulateMapCount[K](oldMap: Map[K, Int], newMap: Map[K, Int]): Map[K, Int] = {
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

      requestsPerPage ++= accumulateMapCount(requestsPerPage,
        all(sortedRequests, UserStatisticsActor.groupByUrl, UserStatisticsActor.mapToCount))
      percentPerPage ++= requestsPerPage.mapValues(size => sizeToPercent(size, requestsPerPage.size))

//      timeAggregation(sortedRequests)
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
   * The top x amount of mapped results from the passed requests.
   * @return Map of page URL to Hits
   */
  def top[K, V](number: Int, requests: List[Request],
               groupBy: (Request) => K,
               mapTo: ((K, List[Request])) => (K, V))(implicit ordering: Ordering[V]): Map[K, V] = {
    @tailrec
    def getMax(workingMap: Map[K, V], returnMap: Map[K, V] = Map.empty): Map[K, V] = workingMap match {
      case map if workingMap.isEmpty => returnMap
      case map if returnMap.size == number => returnMap
      case map =>
        val currentMax @ (maxUrl, _) = map.maxBy { case (_, size) => size }
        getMax(map - maxUrl, returnMap + currentMax)
    }

    getMax(all(requests, groupBy, mapTo))
  }

  /**
   * All requests grouped by the specified request parameter and mapped to the result.
   * @return
   */
  def all[K, V](requests: List[Request],
          groupBy: (Request) => K,
          mapTo: ((K, List[Request])) => (K, V)): Map[K, V] = {
    requests.groupBy(groupBy).map(mapTo)
  }

  // Number of requests per minute of the day
  def requestsPerTime(requests: List[Request]): Map[(Hour, Minute), Int] = {
    
    requests.groupBy(request => {
      val date = new DateTime(request.timestamp)
      (date.getHourOfDay, date.getMinuteOfHour)
    }).map {
      case ((hour, time), reqs) => (hour, time) -> reqs.size
    }
  }

  // Average visit time per URL
  def visitTimePerURL(requests: List[Request]): Map[String, Long] = {
    val sortedRequests = requests.sortBy(_.timestamp)
    val consecutivePairOfRequests: List[(Request, Request)] = sortedRequests zip sortedRequests.tail

    val visitTimePerURL: List[(String, Long)] = consecutivePairOfRequests map {
      case (req1, req2) =>
        req1.url -> (req2.timestamp - req1.timestamp)
    }

    visitTimePerURL.groupBy(_._1).map {
      case (url, list: List[(String, Long)]) => url -> list.map(_._2).sum
    }
  }
/*
*
*
    newTimeAggregation.foreach { case (time, count) =>
      val oldCount = requestsPerMinute(time)
      requestsPerMinute += time -> (oldCount + count)
    }
    requestsPerMinute
    totalVisitTime.foreach { case (url, time) =>
      val oldTime = totalVisitTimePerURL(url)
      totalVisitTimePerURL += url -> (oldTime + time)
    }

    totalVisitTimePerURL
    */
}

object UserStatisticsActor {
  type Hour = Int
  type Minute = Int

  def props: Props = Props[UserStatisticsActor]

  case class Percent(percent: Double) extends Ordered[Percent] {
    override def toString = f"$percent%.2f"

    override def compare(that: Percent): Int =
      this.percent.compare(that.percent)
  }

  val groupByUrl: Request => String = req => req.url
  val groupByBrowser: Request => String = req => req.browser
  val groupByReferrer: Request => String = req => req.referrer
  val groupByTime: Request => (Hour, Minute) = request => {
    val date = new DateTime(request.timestamp)
    (date.getHourOfDay, date.getMinuteOfHour)
  }

  val mapToCount: ((String, List[Request])) => (String, Int) = {
    case (groupName, reqs) => groupName -> reqs.size
  }
  val mapToUserCount: ((String, List[Request])) => (String, Int) = {
    case (groupName, reqs) => groupName -> reqs.groupBy(_.sessionId).size
  }

  def sizeToPercent(size: Int, total: Int): Percent = {
    Percent(size.toDouble / total * 100)
  }

  val mapToCountByTime: ((Hour, Minute), List[Request]) => ((Hour, Minute), Int) = {
    case ((hour, time), reqs) => (hour, time) -> reqs.size
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
