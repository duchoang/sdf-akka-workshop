package com.boldradius.sdf.akka

import akka.actor.Actor.Receive
import akka.actor.{Props, ActorLogging, Actor}
import com.boldradius.sdf.akka.ChatActor.HelpUser

class ChatActor extends Actor with ActorLogging {
  override def receive: Receive = {
    case HelpUser(id) =>
      log.info(s"Hello user $id, do you need live assistance?")
  }
}

object ChatActor {

  case class HelpUser(id: Long)

  def props = Props[ChatActor]

}
