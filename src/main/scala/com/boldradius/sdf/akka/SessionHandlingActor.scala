package com.boldradius.sdf.akka

import java.util.concurrent.TimeUnit

import akka.actor.Actor.Receive
import akka.actor.{FSM, Props, ActorLogging, Actor}
import com.boldradius.sdf.akka.RequestConsumer.GetMetrics
import com.boldradius.sdf.akka.SessionHandlingActor._

import scala.concurrent.duration.FiniteDuration


class SessionHandlingActor(id: Long) extends FSM[SessionState, SessionData] with ActorLogging {

  val timeout = FiniteDuration(
    context.system.settings.config.getDuration("akka-workshop.session-handling-actor.timeout", TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS
  )

  startWith(Active, Requests(List.empty[Request]))

  when(Active) {
    case Event(request: Request, requests: Requests) =>
      setTimer("timeout", InactiveSession(id, requests), timeout)
      log.debug(s"[Active] Actor $id received this $request")
      stay() using requests.copy(list = request :: requests.list)

    case Event(GetMetrics, requests: Requests) =>
      val metrics = Metrics(requests.list.last.url, requests.list.last.browser)
      log.debug(s"[Active] Replying with metrics: $metrics")
      sender() ! metrics
      stay()

    case Event(InactiveSession(_, _), requests: Requests) =>
      log.debug(s"[Inactive] after timeout $timeout, send InactiveSession($id) Msg to parent")
      context.parent ! InactiveSession(id, requests)
      goto(Inactive)
  }

  when(Inactive) {
    case Event(request: Request, _) =>
      log.debug(s"[Inactive] still receive this $request, forward this one to parent")
      context.parent forward request
      stay()
  }

  initialize()

}

object SessionHandlingActor {
  def props(id: Long): Props = Props(new SessionHandlingActor(id))

  sealed trait SessionState
  case object Active extends SessionState
  case object Inactive extends SessionState

  case class InactiveSession(id: Long, requests: Requests)

  sealed trait SessionData
  case class Requests(list: List[Request]) extends SessionData

  case class Metrics(currentUrl: String, browser: String)
}
