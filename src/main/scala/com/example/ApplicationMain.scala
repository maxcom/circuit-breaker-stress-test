package com.example

import akka.actor.Status.Failure
import akka.actor.{Props, ActorLogging, Actor, ActorSystem}
import akka.pattern.{CircuitBreakerOpenException, PipeToSupport, CircuitBreaker}
import com.example.StressActor._
import scala.concurrent.Promise
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Random

object StressActor {
  case object JobDone
  case object Tick
}

class StressActor extends Actor with ActorLogging with PipeToSupport {
  private val breaker = CircuitBreaker(context.system.scheduler, 5, 100 millisecond, 1 seconds)
  import context.dispatcher

  context.system.scheduler.schedule(10 seconds, 10 seconds, self, Tick)

  private var doneCount = 0
  private var failCount = 0
  private var circCount = 0

  private def job = {
    val promise = Promise[JobDone.type]()

    context.system.scheduler.scheduleOnce(Random.nextInt(500).millisecond) {
      promise.success(JobDone)
    }

    promise.future
  }

  override def receive = {
    case JobDone ⇒
      doneCount += 1
      breaker.withCircuitBreaker(job).pipeTo(self)
    case Failure(ex:CircuitBreakerOpenException) ⇒
      circCount += 1
      breaker.withCircuitBreaker(job).pipeTo(self)
    case Failure(_) ⇒
      failCount += 1
      breaker.withCircuitBreaker(job).pipeTo(self)
    case Tick ⇒
      log.info(s"done $doneCount, fail $failCount, circ $circCount")
      doneCount = 0
      failCount = 0
      circCount = 0
  }
}

object ApplicationMain extends App {
  val system = ActorSystem("MyActorSystem")
  val stressActor = system.actorOf(Props(classOf[StressActor]), "stressActor")

  (0 to 1000) foreach { _ ⇒ stressActor ! JobDone }
  // This example app will ping pong 3 times and thereafter terminate the ActorSystem - 
  // see counter logic in PingActor
  system.awaitTermination()
}