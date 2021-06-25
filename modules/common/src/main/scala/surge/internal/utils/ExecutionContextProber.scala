// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.utils

import akka.Done
import akka.actor._
import akka.pattern.pipe
import surge.internal.utils.ExecutionContextProberActor.Messages.Stop
import surge.internal.utils.ExecutionContextProberActor.TimerKeys.{ SendProbesKey, TimeoutKey }

import java.util.UUID
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.{ FiniteDuration, _ }
import scala.concurrent.{ ExecutionContext, Future }
import scala.language.postfixOps

final case class ExecutionContextProberSettings(
    targetEc: ExecutionContext,
    initialDelay: FiniteDuration,
    timeout: FiniteDuration,
    interval: FiniteDuration,
    numProbes: Int)

object ExecutionContextProberActor {

  def props(settings: ExecutionContextProberSettings): Props = Props(new ExecutionContextProberActor(settings))

  object Messages {

    final case object Stop

    final case object SendProbes

    final case object Timeout

    final case object HasIssues // used by the unit test

  }

  object TimerKeys {

    final case object SendProbesKey

    final case object TimeoutKey

  }

  sealed trait ProbingResult

  final case class Ack(id: UUID) extends ProbingResult

  def noOpFuture(id: UUID)(implicit ec: ExecutionContext): Future[ProbingResult] = Future {
    Ack(id)
  }

  val warningText = "Possible thread starvation issue"

}

class ExecutionContextProberActor(settings: ExecutionContextProberSettings) extends Actor with ActorLogging with Timers {

  import ExecutionContextProberActor.Messages._
  import ExecutionContextProberActor._

  implicit val system: ActorSystem = context.system

  var detectedIssue = false // used by the unit test

  override def preStart(): Unit = {
    timers.startSingleTimer(SendProbesKey, SendProbes, settings.initialDelay)
  }

  override def receive: Receive = {
    case SendProbes =>
      val id = UUID.randomUUID()
      implicit val ec: ExecutionContext = settings.targetEc
      (1 to settings.numProbes).foreach(_ => pipe(noOpFuture(id)).to(self))
      timers.startSingleTimer(TimeoutKey, msg = Timeout, settings.timeout)
      context.become(collect(count = 0, expectedId = id))
    case HasIssues =>
      sender() ! detectedIssue
    case Stop =>
      timers.cancel(SendProbesKey)
      sender() ! Done
      context.stop(self)
    case _ => // ignore (the uncancelled futures we didn't collect from previous check(s) ...)
  }

  def collect(count: Int, expectedId: UUID): Receive = {
    case Timeout =>
      log.warning(s"One of our probes timed out.${warningText}.")
      // the execution context is potentially unhealthy so we want to suspend our checks for a bit (to give
      // it time to recover in case it's just a hiccup), hence interval * 3.
      detectedIssue = true
      timers.startSingleTimer(SendProbesKey, msg = SendProbes, settings.interval * 3)
      context.become(receive)
    case Status.Failure(e) =>
      log.error(e, s"One of our probes resulted in a failed future. ${warningText}.")
      detectedIssue = true
      timers.startSingleTimer(SendProbesKey, msg = SendProbes, settings.interval * 3)
      timers.cancel(TimeoutKey)
      context.become(receive)
    case Ack(id: UUID) if id == expectedId =>
      if (count == settings.numProbes - 1) {
        // we successfully collected every probe, so we are done and we schedule the next check
        timers.cancel(TimeoutKey)
        timers.startSingleTimer(SendProbesKey, msg = SendProbes, settings.interval)
        context.become(receive)
      } else {
        context.become(collect(count + 1, expectedId))
      }
    case Stop =>
      timers.cancel(TimeoutKey)
      sender() ! Done
      context.stop(self)
    case HasIssues =>
      sender() ! detectedIssue
    case _ => // ignore (the uncancelled futures we didn't collect from previous check(s) ...)
  }
}

// Akka extension boilerplate
// See: https://doc.akka.io/docs/akka/current/typed/extending.html
class ExecutionContextProberImpl(system: ActorSystem) extends Extension {

  val targetEc = system.dispatcher // the target EC is the main dispatcher of the actor system
  val proberConfig = system.settings.config.getConfig("prober")
  val initialDelay = proberConfig.getDuration("initial-delay", TimeUnit.MILLISECONDS) millis
  val timeout = proberConfig.getDuration("timeout", TimeUnit.MILLISECONDS) millis
  val interval = proberConfig.getDuration("interval", TimeUnit.MILLISECONDS) millis
  val numProbes = proberConfig.getInt("num-probes")

  val settings = ExecutionContextProberSettings(targetEc, initialDelay, timeout, interval, numProbes)

  val actor = system.actorOf(ExecutionContextProberActor.props(settings), name = "prober")

  CoordinatedShutdown(system).addTask(CoordinatedShutdown.PhaseBeforeServiceUnbind, "stopProber") { () =>
    stop()
  }

  def stop(): Future[Done] = {
    import akka.pattern.ask
    import akka.util.Timeout
    implicit val timeout: Timeout = Timeout(3 seconds)
    (actor ? Stop).mapTo[Done]
  }
}

object ExecutionContextProber extends ExtensionId[ExecutionContextProberImpl] with ExtensionIdProvider {

  override def lookup: ExecutionContextProber.type = ExecutionContextProber

  override def createExtension(system: ExtendedActorSystem): ExecutionContextProberImpl = new ExecutionContextProberImpl(system)

}
