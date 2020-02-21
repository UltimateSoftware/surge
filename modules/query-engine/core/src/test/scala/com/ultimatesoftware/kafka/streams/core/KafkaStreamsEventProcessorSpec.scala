// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams.core

import java.time.Instant
import java.util.UUID

import com.ultimatesoftware.kafka.streams.KafkaStreamsTestHelpers
import com.ultimatesoftware.scala.core.domain.{ BasicStateTypeInfo, StateMessage }
import com.ultimatesoftware.scala.core.kafka.{ JsonSerdes, KafkaTopic }
import com.ultimatesoftware.scala.core.messaging.EventProperties
import com.ultimatesoftware.scala.core.utils.JsonUtils
import com.ultimatesoftware.scala.oss.domain.AggregateSegment
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.streams.test.ConsumerRecordFactory
import org.scalatest.{ Matchers, WordSpec }
import play.api.libs.json.{ Format, Json }

class KafkaStreamsEventProcessorSpec extends WordSpec with Matchers with KafkaStreamsTestHelpers {
  private val eventTopic: KafkaTopic = KafkaTopic("testEventTopic")

  case class ExampleAgg(aggId: String, state: String, count: Int)
  case class ExampleEvent(aggId: String, newState: String) {
    def toAgg(withCount: Int): ExampleAgg = ExampleAgg(aggId, newState, withCount)
  }
  private implicit val aggFormat: Format[ExampleAgg] = Json.format
  private implicit val eventFormat: Format[ExampleEvent] = Json.format
  private val readFormatting = new SurgeReadFormatting[String, StateMessage[ExampleAgg], ExampleEvent, EventProperties] {
    override def readEvent(bytes: Array[Byte]): (ExampleEvent, Option[EventProperties]) = {
      val event = JsonUtils.parseMaybeCompressedBytes[ExampleEvent](bytes)
      val evtProps = EventProperties(UUID.randomUUID(), UUID.randomUUID(), None, Some(event.get.aggId), 1, Instant.now, Instant.now, None, None,
        metadata = Map.empty)
      event.get -> Some(evtProps)
    }
    override def readState(bytes: Array[Byte]): Option[AggregateSegment[String, StateMessage[ExampleAgg]]] = {
      val unzipped = JsonUtils.parseMaybeCompressedBytes[StateMessage[ExampleAgg]](bytes)
      unzipped.map(agg ⇒ AggregateSegment(agg.body.get.aggId, Json.toJson(agg), Some(classOf[ExampleAgg])))
    }
  }
  private val writeFormatting = new SurgeAggregateWriteFormatting[String, StateMessage[ExampleAgg]] {
    override def writeState(agg: AggregateSegment[String, StateMessage[ExampleAgg]]): Array[Byte] = {
      JsonUtils.gzip(agg.value)
    }
  }

  private def eventHandler(oldAgg: Option[ExampleAgg], event: ExampleEvent, props: EventProperties): Option[ExampleAgg] = {
    val incrementedCount = oldAgg.map(_.count).getOrElse(0) + 1
    Some(event.toAgg(incrementedCount))
  }

  private val eventProcessor = new KafkaStreamsEventProcessor[String, ExampleAgg, ExampleEvent, EventProperties](
    "exampleAgg",
    BasicStateTypeInfo("exampleAgg", "1.0"), readFormatting, writeFormatting, eventTopic, None, eventHandler)

  private def extractStateFromStore(bytes: Array[Byte]): Option[ExampleAgg] = {
    readFormatting.readState(bytes)
      .flatMap(segment ⇒ segment.value.asOpt[StateMessage[ExampleAgg]])
      .flatMap(_.body)
  }

  "KafkaStreamsEventProcessor" should {
    "Store key value pairs from Kafka in a KTable" in withTopologyTestDriver(eventProcessor.createTopology()) { testDriver ⇒
      val store = testDriver.getKeyValueStore[String, Array[Byte]](eventProcessor.aggregateKTableStoreName)
      val factory = new ConsumerRecordFactory[String, ExampleEvent](eventTopic.name, new StringSerializer(),
        JsonSerdes.serdeFor[ExampleEvent].serializer())

      val event1 = ExampleEvent("agg1", "state1")
      val update1 = ExampleEvent("agg1", "updatedState")
      val event2 = ExampleEvent("agg2", "otherState")

      testDriver.pipeInput(factory.create(eventTopic.name, s"${event1.aggId}:1", event1))
      testDriver.pipeInput(factory.create(eventTopic.name, s"${event2.aggId}:1", event2))
      extractStateFromStore(store.get(event1.aggId)) shouldEqual Some(event1.toAgg(1))
      extractStateFromStore(store.get(event2.aggId)) shouldEqual Some(event2.toAgg(1))

      testDriver.pipeInput(factory.create(eventTopic.name, s"${update1.aggId}:2", update1))
      extractStateFromStore(store.get(event1.aggId)) shouldEqual Some(update1.toAgg(2))
    }
  }
}
