package com.boldradius.sdf.akka

import akka.actor.Actor.Receive
import akka.actor.{Props, ActorLogging, Actor}
import com.boldradius.sdf.akka.EmailActor.StatsActorTerminated

class EmailActor extends Actor with ActorLogging {

  override def receive: Receive = {
    case StatsActorTerminated(reason) =>
      log.error(reason)
  }

}

object EmailActor {

  def props = Props[EmailActor]

  case class StatsActorTerminated(reason: String)
}
