package com.boldradius.sdf.akka

import akka.actor._
import akka.util.Timeout
import com.boldradius.sdf.akka.EmailActor.StatsActorTerminated
import com.boldradius.sdf.akka.RequestConsumer.GetMetrics
import com.boldradius.sdf.akka.SessionHandlingActor.{Metrics, InactiveSession}
import akka.pattern.{ask, pipe}

import scala.concurrent.duration._
import scala.concurrent.Future

object RequestConsumer {
  def props = Props[RequestConsumer]

  case object GetMetrics
}

class RequestConsumer extends Actor with ActorLogging {

  implicit val executionContext = context.dispatcher
  implicit val timeout = Timeout(10 seconds)

  override val supervisorStrategy = {
    OneForOneStrategy(maxNrOfRetries = 2)(super.supervisorStrategy.decider)
  }

  private var sessionHandlers = Map.empty[Long, ActorRef]

  val statsActor = context.actorOf(UserStatisticsActor.props, "stats-actor")
  context.watch(statsActor)
  val emailActor = context.actorOf(EmailActor.props, "email-actor")

  def receive: Receive = {
    case request: Request => handleRequest(request)

    case InactiveSession(id, requests) =>
      context.stop(sessionHandlers(id))
      sessionHandlers -= id
      statsActor ! requests

    case Terminated(ref) =>
      emailActor ! StatsActorTerminated(ref.path.name)

    case GetMetrics =>
      val futureMetrics = sessionHandlers.values.map(sessionHandler => (sessionHandler ? GetMetrics).mapTo[Metrics])
      Future.fold[Metrics, List[Metrics]](futureMetrics)(List.empty) { (total, metrics) => metrics :: total } pipeTo sender()
  }

  private def handleRequest(request: Request) = {
    if (sessionHandlers.contains(request.sessionId)) {
      sessionHandlers(request.sessionId) ! request
    } else {
      val sessionHandler = context.actorOf(SessionHandlingActor.props(request.sessionId))
      sessionHandlers += (request.sessionId -> sessionHandler)
      sessionHandler ! request
    }
  }
}
