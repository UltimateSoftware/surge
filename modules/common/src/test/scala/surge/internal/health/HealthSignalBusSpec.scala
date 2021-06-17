// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.health

import akka.Done
import akka.actor.{ ActorRef, ActorSystem }
import akka.testkit.{ TestKit, TestProbe }
import org.mockito.Mockito._
import org.mockito.stubbing.Stubber
import org.mockito.{ ArgumentMatchers, Mockito }
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import org.scalatestplus.mockito.MockitoSugar
import surge.health.domain.{ EmittableHealthSignal, Error, HealthSignal, Trace, Warning }
import surge.health.matchers.SignalPatternMatcher
import surge.health.{ HealthSignalListener, SignalHandler }
import surge.internal.health.context.TestHealthSignalStream
import surge.internal.health.supervisor.HealthSupervisorActorRef

import scala.concurrent.duration._
import scala.util.Try

class TestHealthSignalListener(override val signalBus: HealthSignalBusInternal) extends HealthSignalListener {
  override def id(): String = "test-listener"

  override def start(maybeSideEffect: Option[() => Unit]): HealthSignalListener = this

  override def stop(): HealthSignalListener = this
  override def subscribeWithFilters(signalHandler: SignalHandler, filters: Seq[SignalPatternMatcher]): HealthSignalListener = {
    this.bindSignalHandler(signalHandler)
    signalBus.subscribe(subscriber = this, signalBus.signalTopic())
    this
  }
}

class TestSignalHandler extends SignalHandler {
  override def handle(signal: HealthSignal): Try[Done] = Try { Done }
}

trait MockitoHelper extends MockitoSugar {
  def doReturn(toBeReturned: Any): Stubber = {
    Mockito.doReturn(toBeReturned, Nil: _*)
  }
}

class HealthSignalBusSpec extends TestKit(ActorSystem("healthSignalBus")) with AnyWordSpecLike with Matchers with MockitoHelper {
  import surge.internal.health.context.TestContext._
  val signalStreamProvider: HealthSignalStreamProvider = testHealthSignalStreamProvider(Seq.empty)

  "HealthSignalBus" should {
    "have stream monitoring" in {
      val probe = TestProbe()
      val bus =
        HealthSignalBus(signalStreamProvider)
          .withStreamSupervision(_ => new HealthSupervisorActorRef(probe.ref, 30.seconds, system), Some(new StreamMonitoringRef(probe.ref)))
      bus.supervisor().nonEmpty shouldEqual true

      bus.unsupervise()
    }

    "have a window stream" in {
      signalStreamProvider.busWithSupervision().signalStream() shouldBe a[TestHealthSignalStream]
    }

    "throw runtime exception when handling registration and supervisor not available" in {
      val bus = signalStreamProvider.busWithSupervision()
      val spy = Mockito.spy(bus)

      doReturn(None).when(spy).supervisor()
      a[RuntimeException] should be thrownBy spy.registration(Mockito.mock(classOf[ActorRef]), "foo", Seq.empty, Seq.empty)
    }

    "not fail on repeated supervise calls" in {
      signalStreamProvider.busWithSupervision().supervise().supervise().supervisor().isDefined shouldEqual true
    }

    "not fail on repeated unsupervise calls" in {
      signalStreamProvider.busWithSupervision().supervise().unsupervise().unsupervise().unsupervise().supervisor().isDefined shouldEqual false
    }

    "create trace signal" in {
      val emitSignal: EmittableHealthSignal = HealthSignalBus(signalStreamProvider).signalWithTrace(name = "trace", Trace("a trace", None, None))
      emitSignal.underlyingSignal().name shouldEqual "trace"
      emitSignal.underlyingSignal().data.error shouldEqual None
      emitSignal.underlyingSignal().data.description shouldEqual "a trace"
    }

    "create error signal" in {
      val emitSignal: EmittableHealthSignal = HealthSignalBus(signalStreamProvider).signalWithError(name = "error", Error("an error", None, None))
      emitSignal.underlyingSignal().name shouldEqual "error"
      emitSignal.underlyingSignal().data.error shouldEqual None
      emitSignal.underlyingSignal().data.description shouldEqual "an error"
    }

    "create warning signal" in {
      val emitSignal: EmittableHealthSignal = HealthSignalBus(signalStreamProvider).signalWithWarning(name = "warning", Warning("a warning", None, None))
      emitSignal.underlyingSignal().name shouldEqual "warning"
      emitSignal.underlyingSignal().data.error shouldEqual None
      emitSignal.underlyingSignal().data.description shouldEqual "a warning"
    }

    "be invoked by trace signal when emitting" in {
      val bus = spy(HealthSignalBus(signalStreamProvider))

      val emitSignal: EmittableHealthSignal = bus.signalWithTrace(name = "trace", Trace("a trace", None, None))

      emitSignal.emit()

      verify(bus, times(1)).publish(ArgumentMatchers.any(classOf[HealthSignal]))
    }

    "be invoked by error signal when emitting" in {
      val bus = spy(HealthSignalBus(signalStreamProvider))
      val emitSignal: EmittableHealthSignal = bus.signalWithError(name = "error", Error("an error", None, None))

      emitSignal.emit()

      verify(bus, times(1)).publish(ArgumentMatchers.any(classOf[HealthSignal]))
    }

    "be invoked by warning signal when emitting" in {
      val bus = spy(HealthSignalBus(signalStreamProvider))
      val emitSignal: EmittableHealthSignal = bus.signalWithWarning(name = "warning", Warning("a warning", None, None))

      emitSignal.emit()

      verify(bus, times(1)).publish(ArgumentMatchers.any(classOf[HealthSignal]))
    }

    "invoke listener handle on warning emit" in {
      val bus: HealthSignalBusInternal = spy(HealthSignalBus(signalStreamProvider))

      val listener: HealthSignalListener = testListener(bus)

      val handler: SignalHandler = testSignalHandler()
      val signalHandler = spy(handler)

      listener.subscribeWithFilters(signalHandler)

      val emitSignal: EmittableHealthSignal = bus.signalWithWarning(name = "warning", Warning("a warning", None, None))

      emitSignal.emit()

      verify(signalHandler, times(1)).handle(ArgumentMatchers.any(classOf[HealthSignal]))
    }

    "invoke listener on error emit" in {
      val bus: HealthSignalBusInternal = spy(HealthSignalBus(signalStreamProvider))

      val listener: HealthSignalListener = testListener(bus)
      val handler: SignalHandler = testSignalHandler()
      val signalHandler = spy(handler)

      listener.subscribeWithFilters(signalHandler)

      val emitSignal: EmittableHealthSignal = bus.signalWithError(name = "error", Error("an error", None, None))

      emitSignal.emit()

      verify(signalHandler, times(1)).handle(ArgumentMatchers.any(classOf[HealthSignal]))
    }
  }

  private def testSignalHandler(): SignalHandler = {
    new TestSignalHandler()
  }

  private def testListener(bus: HealthSignalBusInternal): HealthSignalListener = {
    new TestHealthSignalListener(bus)
  }
}
