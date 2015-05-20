package com.boldradius.sdf.akka

import akka.actor.{Props, Actor, ActorLogging}
import com.boldradius.sdf.akka.UserStatisticsActor.Percent


//case class Request(sessionId: Long, timestamp: Long, url: String, referrer: String, browser: String)

class UserStatisticsActor extends Actor with ActorLogging {

  private var allRequests: List[Request] = List()

  // Aggregations
  private var requestsPerBrowser: Map[String, Int] = Map().withDefaultValue(0)
  private var requestsPerPage: Map[String, Percent] = Map.empty

  override def receive: Receive = {
    case SessionHandlingActor.Requests(requests) =>
      allRequests = allRequests ::: requests
      browserUsersAggregation(requests)
      pageVisitsAggregation(allRequests)
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


}

object UserStatisticsActor {
  def props: Props = Props[UserStatisticsActor]

  case class Percent(percent: Double) {
    override def toString = f"$percent%.2f"
  }
}
