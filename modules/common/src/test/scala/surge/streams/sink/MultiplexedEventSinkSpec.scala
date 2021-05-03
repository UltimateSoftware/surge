// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.streams.sink

import akka.actor.ActorSystem
import akka.testkit.{ TestKit, TestProbe }
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import surge.streams._

import scala.concurrent.{ ExecutionContext, Future }

class MultiplexedEventSinkSpec extends TestKit(ActorSystem("MultiplexedEventSink")) with AnyWordSpecLike with Matchers {
  sealed trait TopLevelEvent

  sealed trait Subtype1 extends TopLevelEvent
  case class Subtype1Event1(string: String) extends Subtype1
  case class Subtype1Event2(int: Int) extends Subtype1

  sealed trait Subtype2 extends TopLevelEvent
  case class Subtype2Event1(string: String) extends Subtype2

  case class BothSubtypesMessage(property: String) extends Subtype1 with Subtype2

  class SubtypeEventSink[T](probe: TestProbe) extends EventSink[T] {
    override def handleEvent(key: String, event: T, headers: Map[String, Array[Byte]]): Future[Any] = Future { probe.ref ! event }(ExecutionContext.global)
    override def partitionBy(key: String, event: T, headers: Map[String, Array[Byte]]): String = key
  }
  class Subtype1Adapter(probe: TestProbe) extends EventSinkMultiplexAdapter[TopLevelEvent, Subtype1] {
    override def convertEvent(event: TopLevelEvent): Option[Subtype1] = event match {
      case e: Subtype1 => Some(e)
      case _           => None
    }
    override val sink: EventSink[Subtype1] = new SubtypeEventSink[Subtype1](probe)
  }
  class Subtype2Adapter(probe: TestProbe) extends EventSinkMultiplexAdapter[TopLevelEvent, Subtype2] {
    override def convertEvent(event: TopLevelEvent): Option[Subtype2] = event match {
      case e: Subtype2 => Some(e)
      case _           => None
    }
    override val sink: EventSink[Subtype2] = new SubtypeEventSink[Subtype2](probe)
  }

  "MultiplexedEventSink" should {
    "Properly forward messages to any interested downstream consumers" in {
      val eventSource = new TestEventSource[TopLevelEvent]()
      val subtype1Probe = TestProbe()
      val subtype2Probe = TestProbe()
      val testMultiplexedSink = new MultiplexedEventSink[TopLevelEvent] {
        override implicit def ec: ExecutionContext = ExecutionContext.global
        override def partitionBy(key: String, event: TopLevelEvent, headers: Map[String, Array[Byte]]): String = key
        override def destinationSinks: Seq[EventSinkMultiplexAdapter[TopLevelEvent, _ <: TopLevelEvent]] =
          Seq(new Subtype1Adapter(subtype1Probe), new Subtype2Adapter(subtype2Probe))
      }
      val control = eventSource.to(testMultiplexedSink, "")
      control.sendEvent(Subtype1Event1("subtype-1"))
      subtype1Probe.expectMsg(Subtype1Event1("subtype-1"))
      subtype2Probe.expectNoMessage()

      control.sendEvent(Subtype1Event2(2))
      subtype1Probe.expectMsg(Subtype1Event2(2))
      subtype2Probe.expectNoMessage()

      control.sendEvent(Subtype2Event1("subtype-2"))
      subtype2Probe.expectMsg(Subtype2Event1("subtype-2"))
      subtype1Probe.expectNoMessage()

      control.sendEvent(BothSubtypesMessage("both-subtypes"))
      subtype1Probe.expectMsg(BothSubtypesMessage("both-subtypes"))
      subtype2Probe.expectMsg(BothSubtypesMessage("both-subtypes"))
    }
  }
}
