// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.akka

import akka.actor.{ ActorSystem, NoSerializationVerificationNeeded, Props }
import akka.testkit.{ TestKit, TestProbe }
import io.opentracing.{ References, Span, Tracer }
import io.opentracing.mock.{ MockSpan, MockTracer }
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import surge.tracing.TracedMessage

import scala.jdk.CollectionConverters._

object ProbeWithTraceSupport {
  case object GetMostRecentSpan extends NoSerializationVerificationNeeded
  case class MostRecentSpan(spanOpt: Option[Span]) extends NoSerializationVerificationNeeded
}

class ProbeWithTraceSupport(probe: TestProbe, val tracer: Tracer) extends ActorWithTracing {
  var mostRecentSpan: Option[Span] = None
  override def receive: Receive = {
    case ProbeWithTraceSupport.GetMostRecentSpan =>
      sender() ! ProbeWithTraceSupport.MostRecentSpan(mostRecentSpan)
    case msg =>
      mostRecentSpan = Option(tracer.activeSpan())
      probe.ref.forward(msg)
  }
}

class ActorWithTracingSpec extends TestKit(ActorSystem("ActorWithTracingSpec")) with AnyWordSpecLike with Matchers {
  "ActorWithTracing" should {
    "Directly forward any messages that are not marked as Traced" in {
      val expectedMsg = "Test!"
      val probe = TestProbe()
      val mockTracer = new MockTracer()
      val actor = system.actorOf(Props(new ProbeWithTraceSupport(probe, mockTracer)))

      Option(mockTracer.activeSpan()) shouldEqual None

      actor ! expectedMsg
      probe.expectMsg(expectedMsg)
    }

    "Unwrap the span context and forward the wrapped message for TraceMessages" in {
      val expectedMsg = "Test!"
      val probe = TestProbe()
      val mockTracer = new MockTracer()
      val actor = system.actorOf(Props(new ProbeWithTraceSupport(probe, mockTracer)))

      val testSpan = mockTracer.buildSpan("test_span").start()
      actor ! TracedMessage(mockTracer, expectedMsg, testSpan)
      probe.expectMsg(expectedMsg)

      probe.send(actor, ProbeWithTraceSupport.GetMostRecentSpan)
      val internalSpan = probe.expectMsgClass(classOf[ProbeWithTraceSupport.MostRecentSpan])
      internalSpan.spanOpt.isDefined shouldEqual true
      internalSpan.spanOpt match {
        case Some(mockSpan: MockSpan) =>
          val spanReferences = mockSpan.references()
          spanReferences.size() shouldEqual 1
          val reference = spanReferences.get(0)
          reference.getReferenceType shouldEqual References.FOLLOWS_FROM
          reference.getContext.spanId() shouldEqual testSpan.context().spanId()
          reference.getContext.traceId() shouldEqual testSpan.context().traceId()
        case other =>
          fail(s"Expected a MockSpan but received a class of type ${other.getClass}")
      }
    }
  }
}
