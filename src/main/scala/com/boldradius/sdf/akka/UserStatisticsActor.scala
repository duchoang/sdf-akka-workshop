package com.boldradius.sdf.akka

import akka.actor.{Props, Actor, ActorLogging}


//case class Request(sessionId: Long, timestamp: Long, url: String, referrer: String, browser: String)

class UserStatisticsActor extends Actor with ActorLogging {

  private var allRequests: List[Request] = List()

  private var requestsPerBrowser: Map[String, Int]  = Map().withDefaultValue(0)


  override def receive: Receive = {
    case SessionHandlingActor.Requests(listRequest) =>
      allRequests = allRequests ::: listRequest
      listRequest.groupBy(_.browser).map(tuple => (tuple._1, tuple._2.size)) foreach {
        case (browser, newNum) =>
          val oldNum = requestsPerBrowser(browser)
          requestsPerBrowser += browser -> (oldNum + newNum)
      }
  }


}

object UserStatisticsActor {
  def props: Props = Props[UserStatisticsActor]
}
