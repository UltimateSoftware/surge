// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.tracing

import akka.actor.{ ActorSystem, Props }
import akka.testkit.{ TestKit, TestProbe }
import io.opentracing.{ References, Tracer }
import io.opentracing.mock.{ MockSpan, MockTracer }
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import akka.actor.NoSerializationVerificationNeeded

object ProbeWithTraceSupport {
  case object GetMostRecentSpan extends NoSerializationVerificationNeeded
  case class MostRecentSpan(spanOpt: Option[ActorReceiveSpan]) extends NoSerializationVerificationNeeded
}

class ProbeWithTraceSupport(probe: TestProbe, val tracer: Tracer) extends ActorWithTracing {
  var mostRecentSpan: Option[ActorReceiveSpan] = None
  override def receive: Receive = traceableMessages { actorSpan =>
    { case msg: String =>
      mostRecentSpan = Some(actorSpan)
      probe.ref.forward(msg)
    }
  }.orElse {
    case ProbeWithTraceSupport.GetMostRecentSpan =>
      sender() ! ProbeWithTraceSupport.MostRecentSpan(mostRecentSpan)
    case msg: String =>
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

      val testSpan = mockTracer.buildSpan("parent span").start()
      actor ! TracedMessage(expectedMsg, messageName = "test", testSpan)(mockTracer)
      probe.expectMsg(expectedMsg)
      testSpan.finish()

      probe.send(actor, ProbeWithTraceSupport.GetMostRecentSpan)
      val internalSpan = probe.expectMsgClass(classOf[ProbeWithTraceSupport.MostRecentSpan])
      internalSpan.spanOpt.isDefined shouldEqual true
      internalSpan.spanOpt.get.getUnderlyingSpan shouldBe a[MockSpan]
      val mockSpan = internalSpan.spanOpt.get.getUnderlyingSpan.asInstanceOf[MockSpan]
      val spanReferences = mockSpan.references()
      spanReferences.size() shouldEqual 1
      val parentSpan = spanReferences.get(0)
      parentSpan.getReferenceType shouldEqual References.CHILD_OF
      parentSpan.getContext.spanId() shouldEqual testSpan.context().spanId()
      parentSpan.getContext.traceId() shouldEqual testSpan.context().traceId()
    }
  }
}
