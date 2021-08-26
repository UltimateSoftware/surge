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
    targetEcName: String,
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

  override def preStart(): Unit = {
    timers.startSingleTimer(SendProbesKey, SendProbes, settings.initialDelay)
  }

  override def receive: Receive = ready(detectedIssue = false)

  /**
   * @param detectedIssue
   *   have we detected a starvation issue so far ? used solely by the unit test
   */
  def ready(detectedIssue: Boolean): Receive = {
    case SendProbes =>
      val id = UUID.randomUUID()
      implicit val ec: ExecutionContext = settings.targetEc
      (1 to settings.numProbes).foreach(_ => pipe(noOpFuture(id)).to(self))
      timers.startSingleTimer(TimeoutKey, msg = Timeout, settings.timeout)
      context.become(collect(count = 0, expectedId = id, detectedIssue))
    case HasIssues => // used solely by the unit test
      sender() ! detectedIssue
    case Stop =>
      timers.cancel(SendProbesKey)
      sender() ! Done
      context.stop(self)
    case _ => // ignore (the uncancelled futures we didn't collect from previous check(s) ...)
  }

  /**
   * @param count
   *   number of probe responses we have collected so far
   * @param expectedId
   *   expected id of the probe response
   * @param detectedIssue
   *   have we detected a starvation issue so far ? used solely by the unit test
   */
  def collect(count: Int, expectedId: UUID, detectedIssue: Boolean): Receive = {
    case Timeout =>
      log.warning(
        s"One of our (${settings.numProbes}) probes timed out (after ${settings.timeout}) " +
          s"on execution context ${settings.targetEcName}. $warningText.")
      // the execution context is potentially unhealthy so we want to suspend our checks for a bit (to give
      // it time to recover in case it's just a hiccup), hence interval * 3.
      timers.startSingleTimer(SendProbesKey, msg = SendProbes, settings.interval * 3)
      context.become(ready(detectedIssue = true))
    case Status.Failure(e) =>
      log.error(
        e,
        s"One of our (${settings.numProbes}) probes resulted in a failed future " +
          s"on execution context ${settings.targetEcName}. $warningText.")
      timers.startSingleTimer(SendProbesKey, msg = SendProbes, settings.interval * 3)
      timers.cancel(TimeoutKey)
      context.become(ready(detectedIssue = true))
    case Ack(id: UUID) if id == expectedId =>
      if (count == settings.numProbes - 1) {
        // we successfully collected every probe, so we are done and we schedule the next check
        timers.cancel(TimeoutKey)
        timers.startSingleTimer(SendProbesKey, msg = SendProbes, settings.interval)
        context.become(ready(detectedIssue))
      } else {
        context.become(collect(count + 1, expectedId, detectedIssue))
      }
    case Stop =>
      timers.cancel(TimeoutKey)
      sender() ! Done
      context.stop(self)
    case HasIssues => // used solely by the unit test
      sender() ! detectedIssue
    case _ => // ignore (the uncancelled futures we didn't collect from previous check(s) ...)
  }
}

// Akka extension boilerplate
// See: https://doc.akka.io/docs/akka/current/typed/extending.html
class ExecutionContextProberImpl(system: ActorSystem) extends Extension {

  private val targetEcName = "akka.actor.default-dispatcher" // TODO: make this configurable and support multiple ECs
  private val targetEc = system.dispatchers.lookup(targetEcName)
  private val proberConfig = system.settings.config.getConfig("execution-context-prober")
  private val initialDelay = proberConfig.getDuration("initial-delay", TimeUnit.MILLISECONDS) millis
  private val timeout = proberConfig.getDuration("timeout", TimeUnit.MILLISECONDS) millis
  private val interval = proberConfig.getDuration("interval", TimeUnit.MILLISECONDS) millis
  private val numProbes = proberConfig.getInt("num-probes")

  private val settings = ExecutionContextProberSettings(targetEc, targetEcName, initialDelay, timeout, interval, numProbes)

  private val actor = system.actorOf(ExecutionContextProberActor.props(settings).withDispatcher("execution-context-prober.dispatcher"), name = "prober")

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
