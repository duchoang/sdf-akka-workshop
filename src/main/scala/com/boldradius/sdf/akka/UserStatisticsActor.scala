package com.boldradius.sdf.akka

import akka.actor.{Props, Actor, ActorLogging}
import com.boldradius.sdf.akka.UserStatisticsActor.{BrowserUsersAggregation, Aggregation, AggregatorFn}


//case class Request(sessionId: Long, timestamp: Long, url: String, referrer: String, browser: String)

class UserStatisticsActor extends Actor with ActorLogging {

  private var allRequests: List[Request] = List()

  private var aggregations: List[Aggregation] = List.empty

  private var requestsPerBrowser: Map[String, Int]  = Map().withDefaultValue(0)



  override def receive: Receive = {
    case SessionHandlingActor.Requests(listRequest) =>
      allRequests = allRequests ::: listRequest
      val aggregation = BrowserUsersAggregation()
      listRequest.groupBy(_.browser).map(tuple => (tuple._1, tuple._2.size)) foreach {
        case (browser, newNum) =>
          val oldNum = aggregation.value(browser)
          requestsPerBrowser += browser -> (oldNum + newNum)
      }
  }


}

object UserStatisticsActor {
  def props: Props = Props[UserStatisticsActor]

  sealed trait Aggregation
  case class BrowserUsersAggregation(value: Map[String, Int] = Map.empty.withDefaultValue(0)) {
    override def toString = "Users per browser: \n" + value.mkString("\n")
  }
}
