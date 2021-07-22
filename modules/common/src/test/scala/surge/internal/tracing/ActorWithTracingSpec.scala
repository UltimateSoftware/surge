// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.tracing

import akka.actor.{ ActorSystem, NoSerializationVerificationNeeded, Props }
import akka.testkit.{ ImplicitSender, TestKit, TestProbe }
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator
import io.opentelemetry.api.trace.{ Span, Tracer }
import io.opentelemetry.context.propagation.ContextPropagators
import io.opentelemetry.sdk.OpenTelemetrySdk
import io.opentelemetry.sdk.resources.Resource
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter._
import io.opentelemetry.sdk.trace.SdkTracerProvider
import io.opentelemetry.sdk.trace.`export`.{ BatchSpanProcessor, SimpleSpanProcessor }
import io.opentelemetry.sdk.trace.data.SpanData
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.{ Eventually, IntegrationPatience }
import org.scalatest.concurrent.Eventually.eventually
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import surge.internal.akka.ActorWithTracing
import surge.internal.tracing.ProbeWithTraceSupport.{ GetMostRecentSpan, MostRecentSpan, TestMessage }
import surge.internal.tracing.TracingHelper.{ SpanBuilderExt, SpanExt, TracerExt }

object ProbeWithTraceSupport {
  case object TestMessage
  case object GetMostRecentSpan extends NoSerializationVerificationNeeded
  case class MostRecentSpan(spanOpt: Option[Span]) extends NoSerializationVerificationNeeded
}

class ProbeWithTraceSupport(probe: TestProbe, val tracer: Tracer) extends ActorWithTracing {

  override def messageNameForTracedMessages: MessageNameExtractor = { case TestMessage =>
    "TestMessage" // instead of TestMessage$
  }

  var mostRecentSpan: Option[Span] = None
  override def receive: Receive = {
    case ProbeWithTraceSupport.GetMostRecentSpan =>
      sender() ! ProbeWithTraceSupport.MostRecentSpan(mostRecentSpan)
    case msg =>
      mostRecentSpan = Some(activeSpan)
      probe.ref.forward(msg)

  }

}

class ActorWithTracingSpec
    extends TestKit(ActorSystem("ActorWithTracingSpec"))
    with AnyWordSpecLike
    with Matchers
    with BeforeAndAfterAll
    with ImplicitSender
    with Eventually
    with IntegrationPatience {

  val exporter = create() // in memory exporter for OpenTelemetry

  val tracer = {

    val sdkTracerProvider = SdkTracerProvider
      .builder()
      .addSpanProcessor(SimpleSpanProcessor.create(exporter))
      .setResource(Resource.builder().put("service.name", "simple").build())
      .build()

    val openTelemetry: OpenTelemetrySdk = OpenTelemetrySdk
      .builder()
      .setTracerProvider(sdkTracerProvider)
      .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
      .buildAndRegisterGlobal()

    openTelemetry.getTracer(OpenTelemetryInstrumentation.Name, OpenTelemetryInstrumentation.Version)

  }

  override def afterAll(): Unit = {
    exporter.shutdown()
    TestKit.shutdownActorSystem(system, verifySystemShutdown = true)
  }

  "ActorWithTracing" should {
    "Directly forward any messages that are not marked as TracedMessage (but still create an orphan span)" in {
      val expectedMsg = TestMessage
      val probe = TestProbe()
      val actor = system.actorOf(Props(new ProbeWithTraceSupport(probe, tracer)))
      actor ! expectedMsg
      probe.expectMsg(expectedMsg)
      actor ! GetMostRecentSpan
      expectMsgPF() { case MostRecentSpan(Some(_)) =>
      }

      eventually {
        exporter.getFinishedSpanItems.get(0).getName shouldBe "ProbeWithTraceSupport:TestMessage"
        exporter.getFinishedSpanItems.get(1).getName shouldBe "ProbeWithTraceSupport:GetMostRecentSpan$"
      }

    }

    "Unwrap and forward a TracedMessage (and create a span that has a proper parent)" in {
      val expectedMsg = TestMessage
      val probe = TestProbe()
      val actor = system.actorOf(Props(new ProbeWithTraceSupport(probe, tracer)))
      val parentSpan = tracer.buildSpan("The Parent Span").start()
      actor ! TracedMessage(expectedMsg, parentSpan)(tracer)
      probe.expectMsg(expectedMsg)
      parentSpan.finish()
      actor ! GetMostRecentSpan

      var expectedSpan =
        expectMsgPF[Span]() { case MostRecentSpan(Some(s)) =>
          s
        }

      eventually {
        import scala.jdk.CollectionConverters._
        val finishedSpanItems: List[SpanData] = exporter.getFinishedSpanItems.asScala.toList
        val maybeItem = finishedSpanItems.find(item => item.getSpanId == expectedSpan.getSpanContext.getSpanId)
        maybeItem.isDefined shouldBe true
        maybeItem.get.getParentSpanId shouldBe parentSpan.getSpanContext.getSpanId
      }

    }
  }
}
