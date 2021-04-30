// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.internal.streams

import akka.Done
import akka.actor.{ ActorSystem, Props }
import akka.testkit.{ TestKit, TestProbe }
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import org.scalatestplus.mockito.MockitoSugar
import surge.internal.akka.cluster.{ ActorRegistry, ActorSystemHostAwareness }
import surge.internal.streams.ReplayCoordinator.{ ReplayCompleted, ReplayFailed, StartReplay }
import surge.kafka.HostPort
import surge.streams.replay.EventReplayStrategy

import scala.concurrent.{ ExecutionContext, Future }

class ReplayCoordinatorSpec extends TestKit(ActorSystem("ReplayCoordinatorSpec")) with AnyWordSpecLike with Matchers
  with MockitoSugar with ActorSystemHostAwareness {

  override def actorSystem: ActorSystem = system

  private case object PreReplayCalled
  private case object PostReplayCalled
  private case class ReplayCalled(consumerGroup: String, partitions: Iterable[Int])
  private def mockReplayStrategy(probe: TestProbe): EventReplayStrategy = new EventReplayStrategy {
    override def preReplay: () => Future[Any] = { () =>
      probe.ref ! PreReplayCalled
      Future.successful(Done)
    }
    override def postReplay: () => Unit = { () =>
      probe.ref ! PostReplayCalled
    }
    override def replay(consumerGroup: String, partitions: Iterable[Int]): Future[Done] = {
      probe.ref ! ReplayCalled(consumerGroup, partitions)
      Future.successful(Done)
    }
  }

  "ReplayCoordinator" should {
    "Properly shut down the StreamManager for a topic before replaying" in {
      val testTopic = "test-topic"
      val testConsumerGroup = "test-consumer"
      val testProbe = TestProbe()
      val replayProbe = TestProbe()
      val mockRegistry = mock[ActorRegistry]
      val streamManagerProbe = TestProbe()
      val replayCoordinator = system.actorOf(Props(new ReplayCoordinator(testTopic, testConsumerGroup, mockReplayStrategy(replayProbe), mockRegistry)))
      when(mockRegistry.discoverActors(anyString, any[List[HostPort]], any[List[String]])(any[ExecutionContext]))
        .thenReturn(Future.successful(List(streamManagerProbe.ref.path.toString)))

      streamManagerProbe.expectNoMessage()
      testProbe.send(replayCoordinator, StartReplay)
      replayProbe.expectNoMessage()
      streamManagerProbe.expectMsg(KafkaStreamManagerActor.StopConsuming)
      streamManagerProbe.reply(KafkaStreamManagerActor.SuccessfullyStopped(localAddress, streamManagerProbe.ref))

      replayProbe.expectMsg(PreReplayCalled)
      replayProbe.expectMsg(ReplayCalled(testConsumerGroup, List.empty))
      replayProbe.expectMsg(PostReplayCalled)

      testProbe.expectMsg(ReplayCompleted)
    }

    "Stop and fail the replay if no StreamManagers are found for a topic" in {
      val testTopic = "test-topic"
      val testConsumerGroup = "test-consumer"
      val testProbe = TestProbe()
      val replayProbe = TestProbe()
      val mockRegistry = mock[ActorRegistry]
      val replayCoordinator = system.actorOf(Props(new ReplayCoordinator(testTopic, testConsumerGroup, mockReplayStrategy(replayProbe), mockRegistry)))
      when(mockRegistry.discoverActors(anyString, any[List[HostPort]], any[List[String]])(any[ExecutionContext]))
        .thenReturn(Future.successful(List.empty))

      testProbe.send(replayCoordinator, StartReplay)
      replayProbe.expectNoMessage()
      testProbe.expectMsgType[ReplayFailed]
    }
  }
}
