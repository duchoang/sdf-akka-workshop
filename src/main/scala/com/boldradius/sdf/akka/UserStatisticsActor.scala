package com.boldradius.sdf.akka

import akka.actor.{Props, Actor, ActorLogging}


//case class Request(sessionId: Long, timestamp: Long, url: String, referrer: String, browser: String)

class UserStatisticsActor extends Actor with ActorLogging {

  private var allRequests: List[Request] = List()

  // Aggregations
  private var requestsPerBrowser: Map[String, Int] = Map().withDefaultValue(0)

  override def receive: Receive = {
    case SessionHandlingActor.Requests(requests) =>
      allRequests = allRequests ::: requests
      browserUsersAggregation(requests)
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


}

object UserStatisticsActor {
  def props: Props = Props[UserStatisticsActor]
}
