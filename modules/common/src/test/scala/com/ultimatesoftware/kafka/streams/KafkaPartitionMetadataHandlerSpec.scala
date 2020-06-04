// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams

import akka.actor.{ Actor, ActorSystem, Props }
import akka.testkit.TestKit
import com.ultimatesoftware.kafka.streams.KafkaPartitionMetadataHandlerImpl.KafkaPartitionMetadataUpdated
import com.ultimatesoftware.scala.core.kafka.JsonSerdes
import com.ultimatesoftware.support.Logging
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.scala.StreamsBuilder
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import org.scalatest.concurrent.{ Eventually, PatienceConfiguration }
import org.scalatest.time.{ Millis, Seconds, Span }
import org.scalatestplus.mockito.MockitoSugar
import org.mockito.Mockito._
import org.mockito.ArgumentCaptor

class KafkaPartitionMetadataHandlerSpec
  extends TestKit(ActorSystem("KafkaPartitionMetadataHandlerSpec"))
  with KafkaStreamsTestHelpers
  with AnyWordSpecLike
  with Eventually
  with Matchers
  with MockitoSugar
  with BeforeAndAfterAll
  with PatienceConfiguration
  with Logging {

  override implicit val patienceConfig = PatienceConfig(
    timeout = Span(3, Seconds), interval = Span(10, Millis)) // scalastyle:ignore magic.number

  override def afterAll(): Unit = {
    system.terminate()
    super.afterAll()
  }

  class SubscribedActor(notifyTo: KafkaPartitionMetadataUpdated ⇒ Unit) extends Actor with Logging {
    override def receive: Receive = {
      case value: KafkaPartitionMetadataUpdated ⇒
        notifyTo(value)
      case _ ⇒
    }

    override def preStart(): Unit = {
      context.system.eventStream.subscribe(self, classOf[KafkaPartitionMetadataUpdated])
      log.info("SubscribedActor is alive and subscribed to KafkaPartitionMetadataUpdated")
      super.preStart()
    }
  }

  class NotificationReceiver {
    def receive(value: KafkaPartitionMetadataUpdated) = {}
  }

  "KafkaPartitionMetadataHandler" should {
    "Publish KafkaPartitionMetadataUpdated to the eventStream bus" in {
      val notificationReceiver = mock[NotificationReceiver]
      system.actorOf(Props(new SubscribedActor(notificationReceiver.receive)))

      val testTopicName = "test-topic"

      val handler = new KafkaPartitionMetadataHandlerImpl(system)

      val builder = new StreamsBuilder();
      val stream = builder.stream(
        testTopicName)(Consumed.`with`(Serdes.String(), JsonSerdes.serdeFor[KafkaPartitionMetadata]))
      handler.processPartitionMetadata(stream)
      val topology = builder.build()

      withTopologyTestDriver(topology) { testDriver ⇒
        val inputTopic = testDriver.createInputTopic(testTopicName, Serdes.String().serializer(), JsonSerdes.serdeFor[KafkaPartitionMetadata].serializer())
        val meta = KafkaPartitionMetadata(testTopicName, 0, 10L, "foo")
        inputTopic.pipeInput(meta.topicPartition, meta)
        val expectedMetadataUpdatedEvent = KafkaPartitionMetadataUpdated(
          KafkaPartitionMetadata(meta.topic, meta.partition, meta.offset, meta.key))
        val argumentCaptor = ArgumentCaptor.forClass(classOf[KafkaPartitionMetadataUpdated])
        eventually {
          verify(notificationReceiver).receive(argumentCaptor.capture())
          assert(argumentCaptor.getValue equals (expectedMetadataUpdatedEvent))
        }
      }
    }
  }
}
