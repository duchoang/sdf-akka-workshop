package com.boldradius.sdf.akka

import akka.actor.ActorSystem
import akka.util.Timeout
import com.boldradius.sdf.akka.RequestConsumer.GetMetrics
import com.boldradius.sdf.akka.RequestProducer._
import com.boldradius.sdf.akka.SessionHandlingActor.Metrics
import scala.annotation.tailrec
import scala.concurrent.Await
import scala.io.StdIn
import scala.concurrent.duration._
import akka.pattern.ask

object RequestSimulationExampleApp extends App {

  implicit val timeout = Timeout(10 seconds)

  // First, we create an actor system, a producer and a consumer
  val system = ActorSystem("EventProducerExample")
  val producer = system.actorOf(RequestProducer.props(100), "producerActor")
  val consumer = system.actorOf(RequestConsumer.props, "dummyConsumer")

  // Tell the producer to start working and to send messages to the consumer
  producer ! Start(consumer)

  // Wait for the user to hit <enter>
  println("Hit <enter> to stop the simulation")
  @tailrec
  def getInput: String = {
    StdIn.readLine() match {
      case "status" =>
        val result = Await.result((consumer ? GetMetrics).mapTo[List[Metrics]], 10 seconds)
        val resultString = s"Current Results:\nUser Count: ${result.size}\n" +
          s"Users per URL:\n${result.groupBy(_.currentUrl).mapValues(_.size).mkString("\n")}\n" +
          s"Users per browser:\n${result.groupBy(_.browser).mapValues(_.size).mkString("\n")}"
        println(resultString)
        getInput
      case _ =>
        "Stopping..."
    }
  }
  println(getInput)

  // Tell the producer to stop working
  producer ! Stop

  // Terminate all actors and wait for graceful shutdown
  system.shutdown()
  system.awaitTermination(10 seconds)
}
