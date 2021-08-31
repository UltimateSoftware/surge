// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.streams.sink

import akka.actor.ActorSystem
import akka.stream.scaladsl.{ Keep, Sink }
import akka.stream.testkit.TestPublisher
import akka.stream.testkit.scaladsl.TestSource
import surge.internal.tracing.NoopTracerFactory
import surge.streams.replay.{ NoOpEventReplayControl, ReplayControl, ReplayProgress }
import surge.streams.{ DataPipeline, EventHandler, EventPlusStreamMeta, EventSource }

import scala.concurrent.Future

class TestEventSource[Event](implicit system: ActorSystem) extends EventSource[Event] {
  override def to(sink: EventHandler[Event], consumerGroup: String): TestDataPipeline[Event] = connectSourceToSink(sink)
  override def to(sink: EventHandler[Event], consumerGroup: String, autoStart: Boolean): TestDataPipeline[Event] = connectSourceToSink(sink)

  private def connectSourceToSink(sink: EventHandler[Event]): TestDataPipeline[Event] = {
    val probe = TestSource.probe[EventPlusStreamMeta[String, Event, String]].toMat(sink.eventHandler.to(Sink.ignore))(Keep.left).run()
    new TestDataPipeline[Event](probe)
  }
}

class TestDataPipeline[Event](probe: TestPublisher.Probe[EventPlusStreamMeta[String, Event, String]]) extends DataPipeline {
  override def start(): Unit = {}
  override def stop(): Unit = {}
  override def replay(): Future[DataPipeline.ReplayResult] = Future.successful(DataPipeline.ReplaySuccessfullyStarted())

  def sendEvent(event: Event): DataPipeline = {
    val tracer = NoopTracerFactory.create()
    val span = tracer.spanBuilder("sendEvent (TestDataPipeline)").setNoParent().startSpan()
    val eventPlusStreamMeta = EventPlusStreamMeta("", event, "", Map.empty, span)
    probe.sendNext(eventPlusStreamMeta)
    this
  }

  override def getReplayControl: ReplayControl = new NoOpEventReplayControl()
}
