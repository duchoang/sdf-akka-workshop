package com.boldradius.sdf.akka

import akka.actor._
import com.boldradius.sdf.akka.EmailActor.StatsActorTerminated
import com.boldradius.sdf.akka.SessionHandlingActor.InactiveSession

object RequestConsumer {
  def props = Props[RequestConsumer]
}

class RequestConsumer extends Actor with ActorLogging {

  override val supervisorStrategy = {
    OneForOneStrategy(maxNrOfRetries = 2)(super.supervisorStrategy.decider)
  }

  private var sessionHandlers = Map.empty[Long, ActorRef]

  val statsActor = context.actorOf(UserStatisticsActor.props, "stats-actor")
  context.watch(statsActor)
  val emailActor = context.actorOf(EmailActor.props, "email-actor")

  def receive: Receive = {
    case request: Request =>
      log.debug(s"handling this $request")
      handleRequest(request)

    case InactiveSession(id, requests) =>
      log.debug(s"Inactivesession at $id with all requests:\n:${requests.list.mkString("\n")}")
      context.stop(sessionHandlers(id))
      sessionHandlers -= id
      statsActor ! requests

    case Terminated(ex) =>
      log.debug(s"Terminated($ex)")
      emailActor ! StatsActorTerminated(ex.toString())
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
