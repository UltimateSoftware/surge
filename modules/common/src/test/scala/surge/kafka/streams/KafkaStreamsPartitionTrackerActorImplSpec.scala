// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.kafka.streams

import akka.Done
import akka.actor.ActorSystem
import akka.testkit.{ TestKit, TestProbe }
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.state.{ HostInfo, StreamsMetadata }
import org.mockito.Mockito._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import org.scalatestplus.mockito.MockitoSugar
import surge.internal.akka.kafka.KafkaConsumerStateTrackingActor
import surge.kafka.HostPort

import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters._

class KafkaStreamsPartitionTrackerActorImplSpec
    extends TestKit(ActorSystem("KafkaStreamsPartitionTrackerActorImplSpec"))
    with AnyWordSpecLike
    with Matchers
    with MockitoSugar
    with BeforeAndAfterAll {

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system, duration = 15.seconds, verifySystemShutdown = true)
  }

  private val tp0 = new TopicPartition("testTopic", 0)
  private val tp1 = new TopicPartition("testTopic", 1)
  private val tp2 = new TopicPartition("testTopic", 2)
  private val tp3 = new TopicPartition("testTopic", 3)

  "KafkaStreamsPartitionTrackerActorImpl" should {
    "Be able to follow and update partition assignments" in {
      val probe = TestProbe()
      val hostPort1 = HostPort("host1", 1)
      val hostPort2 = HostPort("host2", 2)
      val assignedPartitions1 = List(tp1, tp2, tp3)
      val assignedPartitions2 = List(tp0)

      val partitionAssignments = Map(hostPort1 -> assignedPartitions1, hostPort2 -> assignedPartitions2)

      val testStreamsMeta = List(
        new StreamsMetadata(
          new HostInfo(hostPort1.host, hostPort1.port),
          Set("store1", "store2").asJava,
          assignedPartitions1.toSet.asJava,
          Set.empty[String].asJava,
          Set.empty[TopicPartition].asJava),
        new StreamsMetadata(
          new HostInfo(hostPort2.host, hostPort2.port),
          Set("store1").asJava,
          assignedPartitions2.toSet.asJava,
          Set.empty[String].asJava,
          Set.empty[TopicPartition].asJava))

      val mockStreams = mock[KafkaStreams]
      when(mockStreams.allMetadata()).thenReturn(testStreamsMeta.asJava)

      val trackerImpl = new KafkaStreamsPartitionTrackerActorProvider(probe.ref).create(mockStreams)
      trackerImpl.update()
      probe.expectMsg(KafkaConsumerStateTrackingActor.StateUpdated(partitionAssignments))
      probe.reply(Done)
    }
  }
}
