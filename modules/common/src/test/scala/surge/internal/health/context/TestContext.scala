// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.health.context

import akka.Done
import akka.actor.ActorSystem
import surge.health.domain.HealthSignal
import surge.health.{ HealthSignalListener, HealthSignalStream, SignalHandler }
import surge.health.matchers.SignalPatternMatcher
import surge.internal.health._

import scala.util.Try

object TestContext {
  def testHealthSignalStreamProvider(filters: Seq[SignalPatternMatcher]): HealthSignalStreamProvider = {
    new TestHealthSignalStreamProvider(filters)
  }
}

class TestHealthSignalStream(bus: HealthSignalBusInternal, matchers: Seq[SignalPatternMatcher]) extends HealthSignalStream {
  override def signalHandler: SignalHandler = (_: HealthSignal) =>
    Try[Done] {
      Done
    }

  override def signalBus(): HealthSignalBusInternal = bus

  override def id(): String = "test-health-signal-bus"

  override def filters(): Seq[SignalPatternMatcher] = matchers

  override def start(maybeSideEffect: Option[() => Unit] = None): HealthSignalStream = {
    subscribeWithFilters(signalHandler, filters())
    maybeSideEffect.foreach(m => m())
    this
  }

  override def stop(): HealthSignalStream = {
    unsubscribe()
    this
  }

  override def subscribeWithFilters(signalHandler: SignalHandler, filters: Seq[SignalPatternMatcher]): HealthSignalListener = {
    bindSignalHandler(signalHandler)
    signalBus().subscribe(subscriber = this, signalBus().signalTopic())
    this
  }
}

class TestHealthSignalStreamProvider(
    override val filters: Seq[SignalPatternMatcher] = Seq.empty,
    override val actorSystem: ActorSystem = ActorSystem("TestHealthSignalStream"))
    extends HealthSignalStreamProvider {
  override def provide(bus: HealthSignalBusInternal): HealthSignalStream =
    new TestHealthSignalStream(bus, this.filters)
}
