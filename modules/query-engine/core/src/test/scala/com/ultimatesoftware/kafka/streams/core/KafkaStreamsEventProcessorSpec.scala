// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams.core

import com.ultimatesoftware.kafka.streams.KafkaStreamsTestHelpers
import com.ultimatesoftware.scala.core.kafka.{ JsonSerdes, KafkaTopic }
import com.ultimatesoftware.scala.core.monitoring.metrics.NoOpMetricsProvider
import org.apache.kafka.common.serialization.StringSerializer
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import play.api.libs.json.{ Format, Json }

class KafkaStreamsEventProcessorSpec extends AnyWordSpec with Matchers with KafkaStreamsTestHelpers {
  private val eventTopic: KafkaTopic = KafkaTopic("testEventTopic")

  case class ExampleAgg(aggId: String, state: String, count: Int)
  case class ExampleEvent(aggId: String, newState: String) {
    def toAgg(withCount: Int): ExampleAgg = ExampleAgg(aggId, newState, withCount)
  }
  case class ExampleMeta(stringProp: String, intProp: Int)

  private implicit val aggFormat: Format[ExampleAgg] = Json.format
  private implicit val eventFormat: Format[ExampleEvent] = Json.format
  private val readFormatting = new SurgeReadFormatting[String, ExampleAgg, ExampleEvent, ExampleMeta] {
    override def readEvent(bytes: Array[Byte]): (ExampleEvent, Option[ExampleMeta]) = {
      val event = Json.parse(bytes).asOpt[ExampleEvent]
      val evtProps = ExampleMeta("string property", 1)
      event.get -> Some(evtProps)
    }
    override def readState(bytes: Array[Byte]): Option[ExampleAgg] = {
      Json.parse(bytes).asOpt[ExampleAgg]
    }
  }
  private val writeFormatting = new SurgeAggregateWriteFormatting[String, ExampleAgg] {
    override def writeState(agg: ExampleAgg): Array[Byte] = {
      Json.toJson(agg).toString().getBytes()
    }
  }

  private def eventHandler(oldAgg: Option[ExampleAgg], event: ExampleEvent, props: ExampleMeta): Option[ExampleAgg] = {
    val incrementedCount = oldAgg.map(_.count).getOrElse(0) + 1
    Some(event.toAgg(incrementedCount))
  }

  private def aggIdExtractor(evtPlusMeta: EventPlusMeta[ExampleEvent, ExampleMeta]): Option[String] = {
    Some(evtPlusMeta.event.aggId)
  }

  private val eventProcessor = new KafkaStreamsEventProcessor[String, ExampleAgg, ExampleEvent, ExampleMeta](
    "exampleAgg", readFormatting, writeFormatting, eventTopic, None, aggIdExtractor, eventHandler, NoOpMetricsProvider)

  private def extractStateFromStore(bytes: Array[Byte]): Option[ExampleAgg] = {
    readFormatting.readState(bytes)
  }

  "KafkaStreamsEventProcessor" should {
    "Store key value pairs from Kafka in a KTable" in withTopologyTestDriver(eventProcessor.createTopology()) { testDriver ⇒
      val store = testDriver.getKeyValueStore[String, Array[Byte]](eventProcessor.aggregateKTableStoreName)
      val inputTopic = testDriver.createInputTopic(eventTopic.name, new StringSerializer, JsonSerdes.serdeFor[ExampleEvent].serializer())

      val event1 = ExampleEvent("agg1", "state1")
      val update1 = ExampleEvent("agg1", "updatedState")
      val event2 = ExampleEvent("agg2", "otherState")

      inputTopic.pipeInput(s"${event1.aggId}:1", event1)
      inputTopic.pipeInput(s"${event2.aggId}:1", event2)
      extractStateFromStore(store.get(event1.aggId)) shouldEqual Some(event1.toAgg(1))
      extractStateFromStore(store.get(event2.aggId)) shouldEqual Some(event2.toAgg(1))

      inputTopic.pipeInput(s"${update1.aggId}:2", update1)
      extractStateFromStore(store.get(event1.aggId)) shouldEqual Some(update1.toAgg(2))
    }
  }
}
