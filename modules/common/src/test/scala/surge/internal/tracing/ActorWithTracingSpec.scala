// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.tracing

import akka.actor.{ ActorSystem, NoSerializationVerificationNeeded, Props }
import akka.testkit.{ TestKit, TestProbe }
import io.opentelemetry.api.trace.{ Span, Tracer }
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import surge.internal.akka.ActorWithTracing
import surge.internal.tracing.TracingHelper.{ SpanBuilderExt, SpanExt, TracerExt }

object ProbeWithTraceSupport {
  case object GetMostRecentSpan extends NoSerializationVerificationNeeded
  case class MostRecentSpan(spanOpt: Option[Span]) extends NoSerializationVerificationNeeded
}

class ProbeWithTraceSupport(probe: TestProbe, val tracer: Tracer) extends ActorWithTracing {
  var mostRecentSpan: Option[Span] = None
  override def receive: Receive = {
    case msg: String =>
      mostRecentSpan = Some(activeSpan)
      probe.ref.forward(msg)
    case ProbeWithTraceSupport.GetMostRecentSpan =>
      sender() ! ProbeWithTraceSupport.MostRecentSpan(mostRecentSpan)
    case msg: String =>
      probe.ref.forward(msg)
  }

}

class ActorWithTracingSpec extends TestKit(ActorSystem("ActorWithTracingSpec")) with AnyWordSpecLike with Matchers {

  val mockTracer = NoopTracerFactory.create()

  "ActorWithTracing" should {
    "Directly forward any messages that are not marked as Traced" in {
      val expectedMsg = "Test!"
      val probe = TestProbe()
      val actor = system.actorOf(Props(new ProbeWithTraceSupport(probe, mockTracer)))
      actor ! expectedMsg
      probe.expectMsg(expectedMsg)
    }

    "Unwrap the span context and forward the wrapped message for TraceMessages" in {
      val expectedMsg = "Test!"
      val probe = TestProbe()
      val actor = system.actorOf(Props(new ProbeWithTraceSupport(probe, mockTracer)))
      val testSpan = mockTracer.buildSpan("parent span").start()
      actor ! TracedMessage(expectedMsg, testSpan)(mockTracer)
      probe.expectMsg(expectedMsg)
      testSpan.finish()
    }
  }
}
