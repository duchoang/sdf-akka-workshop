package com.boldradius.sdf.akka

import akka.actor.{Props, Actor, ActorLogging}
import com.boldradius.sdf.akka.SessionHandlingActor.Requests
import org.joda.time.DateTime
import scala.concurrent.duration._
import akka.persistence._
import scala.annotation.tailrec


import UserStatisticsActor._

case class UserStats(private[akka] var allRequests: List[Request] = List.empty,
                     private[akka] var requestsPerBrowser: Map[String, Int] = Map.empty.withDefaultValue(0),
                     private[akka] var requestsPerPage: Map[String, Int] = Map.empty.withDefaultValue(0),
                     private[akka] var percentPerPage: Map[String, Percent] = Map.empty,
                     private[akka] var requestsPerMinute: Map[(Hour, Minute), Int] = Map.empty.withDefaultValue(0),
                     private[akka] var landingRequests: List[Request] = List.empty,
                     private[akka] var sinkingRequests: List[Request] = List.empty,
                     private[akka] var topLandingPages: Map[String, Int] = Map.empty.withDefaultValue(0),
                     private[akka] var topSinkingPages: Map[String, Int] = Map.empty.withDefaultValue(0),
                     private[akka] var topBrowsersByUser: Map[String, Int] = Map.empty.withDefaultValue(0),
                     private[akka] var topReferrersByUser: Map[String, Int] = Map.empty.withDefaultValue(0),
                     private[akka] var totalVisitTimePerPage: Map[String, Long] = Map.empty.withDefaultValue(0)) {
  def updated(that: UserStats): Unit = {
    allRequests = that.allRequests
    requestsPerBrowser = that.requestsPerBrowser
    requestsPerPage = that.requestsPerPage
    percentPerPage = that.percentPerPage
    requestsPerMinute = that.requestsPerMinute
    landingRequests = that.landingRequests
    sinkingRequests = that.sinkingRequests
    topLandingPages = that.topLandingPages
    topSinkingPages = that.topSinkingPages
    topBrowsersByUser = that.topBrowsersByUser
    topReferrersByUser = that.topReferrersByUser
    totalVisitTimePerPage = that.totalVisitTimePerPage
  }
}


class UserStatisticsActor extends PersistentActor with ActorLogging {

  private val state = UserStats()

  val topPagesCount: Int = context.system.settings.config.getInt("akka-workshop.stats-actor.top-pages")
  val topCounts: Int = context.system.settings.config.getInt("akka-workshop.stats-actor.top-counts")

  log.debug(s"Stat-actor running")

  //  import context.dispatcher
  //  context.system.scheduler.schedule(5 seconds, 3 seconds, self, PersistMsg)


  override def persistenceId = "user-stats-actor"

  override def receiveRecover: Receive = {

    case SnapshotOffer(_, snapshot: UserStats) =>
      log.debug(s"[ReceiveRecover] snapshot=\n$snapshot\n")
      state.updated(snapshot)

    case msg =>
      log.debug(s"[ReceiveRecover] msg = $msg")
  }


  override def receiveCommand: Receive = {

    case req: Requests =>
      log.debug(s"[ReceiveCommand] handleRequests")
      persist(req) { req: Requests => handleRequests(state, req.list)}

    case PersistMsg =>
      log.debug(s"[ReceiveCommand] SaveSnapshot StateTemp $state")
      this.saveSnapshot(state)

    case SaveSnapshotSuccess(metadata)         =>
      log.debug(s"[ReceiveCommand] SaveSnapshotSuccess($metadata)")

    case SaveSnapshotFailure(metadata, reason) =>
      log.debug(s"[ReceiveCommand] SaveSnapshotFailure($metadata, $reason)")
  }

  // update the state
  def handleRequests(state: UserStats, requests: List[Request]): Unit = {
    val sortedRequests = requests.sortBy(req => req.timestamp)

    state.landingRequests = state.landingRequests :+ sortedRequests.head
    state.sinkingRequests = state.sinkingRequests :+ sortedRequests.last
    state.allRequests = state.allRequests ::: sortedRequests

    state.topLandingPages ++= top(topPagesCount, state.landingRequests, UserStatisticsActor.groupByUrl, UserStatisticsActor.mapToCount)
    state.topSinkingPages ++= top(topPagesCount, state.sinkingRequests, UserStatisticsActor.groupByUrl, UserStatisticsActor.mapToCount)

    state.topBrowsersByUser ++= accumulateMapCount(state.topBrowsersByUser,
      top(topCounts, sortedRequests, UserStatisticsActor.groupByBrowser, UserStatisticsActor.mapToUserCount))

    state.topReferrersByUser ++= accumulateMapCount(state.topReferrersByUser,
      top(topCounts, sortedRequests, UserStatisticsActor.groupByReferrer, UserStatisticsActor.mapToUserCount))

    state.requestsPerBrowser ++= accumulateMapCount(state.requestsPerBrowser,
      all(sortedRequests, UserStatisticsActor.groupByBrowser, UserStatisticsActor.mapToCount))

    // Number of requests per minute of the day
    state.requestsPerMinute ++= accumulateMapCount(state.requestsPerMinute,
      all(sortedRequests, UserStatisticsActor.groupByTime, UserStatisticsActor.mapToCountByTime))

    // Number of requests per page
    state.requestsPerPage ++= accumulateMapCount(state.requestsPerPage,
      all(sortedRequests, UserStatisticsActor.groupByUrl, UserStatisticsActor.mapToCount))
    state.percentPerPage ++= state.requestsPerPage.mapValues(size => sizeToPercent(size, state.allRequests.size))

    // Total visit time per Page
    state.totalVisitTimePerPage ++= accumulateMapCount(state.totalVisitTimePerPage, visitTimePerPage(sortedRequests))

  }

  def generateStats: String = {
    val reqPerBrowser = s"Number of requests per browser:\n${state.requestsPerBrowser.toList.mkString("\n")}"
    val ((busyHour, busyMin), countReqs) = state.requestsPerMinute.maxBy(tuple => tuple._2)
    val busyTime = s"Busiest time of the day: $busyHour:$busyMin with #$countReqs requests"
    val reqPerPage = s"Page visit distribution:\n${state.percentPerPage.toList.mkString("\n")}"
    val visitTimePerPage = s"Total visit time per page:\n${state.totalVisitTimePerPage.mkString("\n")}"
    val topLandingPage = s"Top 3 landing pages:\n${state.topLandingPages.mkString("\n")}"
    val topSinkPage = s"Top 3 sink pages:\n${state.topSinkingPages.mkString("\n")}"
    val topBrowser = s"Top 2 browsers:\n${state.topBrowsersByUser.mkString("\n")}"
    val topReferr = s"Top 2 referrers:\n${state.topReferrersByUser.mkString("\n")}"
    reqPerBrowser + "\n" +
      busyTime + "\n" +
      reqPerPage + "\n" +
      visitTimePerPage + "\n" +
      topLandingPage + "\n" +
      topSinkPage + "\n" +
      topBrowser + "\n" +
      topReferr
  }
}

object UserStatisticsActor {

  object PersistMsg

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
  val groupByUrlWithVisitTime: ((String, Long)) => String = tuple => tuple._1

  val mapToCount: ((String, List[Request])) => (String, Int) = {
    case (groupName, reqs) => groupName -> reqs.size
  }
  val mapToUserCount: ((String, List[Request])) => (String, Int) = {
    case (groupName, reqs) => groupName -> reqs.groupBy(_.sessionId).size
  }
  val mapToCountByTime: (((Hour, Minute), List[Request])) => ((Hour, Minute), Int) = {
    case ((hour, time), reqs) => (hour, time) -> reqs.size
  }
  val mapToCountBySumVisitTime: ((String, List[(String, Long)])) => (String, Long) = {
    case (url, list: List[(String, Long)]) => url -> list.map(_._2).sum
  }


  /**
   * The top x amount of mapped results from the passed requests.
   * @return Map of page URL to Hits
   */
  def top[R, K, V](number: Int, requests: List[R],
                   groupBy: R => K,
                   mapTo: ((K, List[R])) => (K, V))(implicit ordering: Ordering[V]): Map[K, V] = {
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
  def all[R, K, V](requests: List[R],
                   groupBy: (R) => K,
                   mapTo: ((K, List[R])) => (K, V)): Map[K, V] = {
    requests.groupBy(groupBy).map(mapTo)
  }

  def visitTimePerPage(requests: List[Request]): Map[String, Long] = {
    val consecutivePairOfRequests: List[(Request, Request)] = requests zip requests.tail
    val visitTimePerURL: List[(String, Long)] = consecutivePairOfRequests map {
      case (req1, req2) => req1.url -> (req2.timestamp - req1.timestamp)
    }
    all(visitTimePerURL, UserStatisticsActor.groupByUrlWithVisitTime, UserStatisticsActor.mapToCountBySumVisitTime)
  }

  def sizeToPercent(size: Int, total: Int): Percent = {
    Percent(size.toDouble / total * 100)
  }

  def accumulateMapCount[K, V](oldMap: Map[K, V], newMap: Map[K, V])(implicit numeric: Numeric[V]): Map[K, V] = {
    newMap map {
      case (key, value) => key -> numeric.plus(oldMap(key), value)
    }
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
