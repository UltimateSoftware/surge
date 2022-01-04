// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.health.context

import akka.Done
import akka.actor.ActorSystem
import surge.health.config.HealthSupervisorConfig
import surge.health.domain.HealthSignal
import surge.health.matchers.SignalPatternMatcherDefinition
import surge.health.{ HealthSignalListener, HealthSignalStream, SignalHandler }
import surge.internal.health._

import scala.util.Try
import scala.concurrent.ExecutionContext

object TestContext {
  def testHealthSignalStreamProvider(patternMatchers: Seq[SignalPatternMatcherDefinition])(implicit ec: ExecutionContext): HealthSignalStreamProvider = {
    new TestHealthSignalStreamProvider(patternMatchers)
  }
}

class TestHealthSignalStream(bus: HealthSignalBusInternal, matchers: Seq[SignalPatternMatcherDefinition], override val actorSystem: ActorSystem)
    extends HealthSignalStream {
  override def signalHandler: SignalHandler = (_: HealthSignal) =>
    Try[Done] {
      Done
    }

  override def signalBus(): HealthSignalBusInternal = bus

  override def id(): String = "test-health-signal-bus"

  override def patternMatchers(): Seq[SignalPatternMatcherDefinition] = matchers

  override def start(maybeSideEffect: Option[() => Unit] = None): HealthSignalStream = {
    subscribe(signalHandler)
    maybeSideEffect.foreach(m => m())
    this
  }

  override def stop(): HealthSignalStream = {
    unsubscribe()
    this
  }

  override def subscribe(signalHandler: SignalHandler): HealthSignalListener = {
    bindSignalHandler(signalHandler)
    signalBus().subscribe(subscriber = this, signalBus().signalTopic())
    this
  }
}

class TestHealthSignalStreamProvider(
    override val patternMatchers: Seq[SignalPatternMatcherDefinition] = Seq.empty,
    override val actorSystem: ActorSystem = ActorSystem("TestHealthSignalStream"))(implicit val ec: ExecutionContext)
    extends HealthSignalStreamProvider {
  override def provide(bus: HealthSignalBusInternal): HealthSignalStream =
    new TestHealthSignalStream(bus, this.patternMatchers, actorSystem)

  override def healthSupervisionConfig: HealthSupervisorConfig = HealthSupervisorConfig()
}
