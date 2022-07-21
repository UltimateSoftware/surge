// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.core

import akka.actor.ActorSystem
import akka.actor.Status.Failure
import akka.pattern.AskTimeoutException
import akka.testkit.{ TestKit, TestProbe }
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.header.internals.RecordHeaders
import org.mockito.Mockito.when
import org.mockito.{ ArgumentMatchers, Mockito }
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually.eventually
import org.scalatest.concurrent.{ PatienceConfiguration, ScalaFutures }
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{ Millis, Seconds, Span }
import org.scalatest.wordspec.AnyWordSpecLike
import org.scalatestplus.mockito.MockitoSugar
import surge.core.KafkaProducerActor.{ PublishSuccess, RetryAwareException }
import surge.health.{ HealthSignalBusTrait, InvokableHealthRegistration }
import surge.internal.akka.actor.ManagedActorRef
import surge.internal.health.{ HealthCheck, HealthCheckStatus, HealthyActor }
import surge.internal.kafka.KafkaProducerActorImpl
import surge.metrics.Metrics

import java.util.UUID
import scala.concurrent.duration._
import scala.concurrent.{ ExecutionContext, Future }

class KafkaProducerActorSpec
    extends TestKit(ActorSystem("KafkaProducerActorSpec"))
    with AnyWordSpecLike
    with Matchers
    with BeforeAndAfterAll
    with TestBoundedContext
    with MockitoSugar
    with ScalaFutures
    with PatienceConfiguration {

  override implicit val patienceConfig: PatienceConfig = PatienceConfig(timeout = Span(3, Seconds), interval = Span(10, Millis))

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system, verifySystemShutdown = true)
  }

  "KafkaProducerActor" should {
    val testMessage = KafkaProducerActor.MessageToPublish(new ProducerRecord("test", 1, "", "".getBytes()))

    def producerMock(testProbe: TestProbe, trackerTimeout: FiniteDuration): KafkaProducerActor = {
      val signalBus: HealthSignalBusTrait = Mockito.mock(classOf[HealthSignalBusTrait])
      val invokable: InvokableHealthRegistration = Mockito.mock(classOf[InvokableHealthRegistration])
      when(invokable.invoke()).thenReturn(Future.successful(Ack))
      when(signalBus.registration(ArgumentMatchers.any(classOf[Controllable]), ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.any()))
        .thenReturn(invokable)
      new KafkaProducerActor(ManagedActorRef(testProbe.ref), Metrics.globalMetricRegistry, "test-aggregate-name", new TopicPartition("testTopic", 1), signalBus)
    }

    "Terminate an underlying actor by sending a PoisonPill" in {
      val probe = TestProbe()
      val shouldBeTerminatedProbe = TestProbe()
      val producer = producerMock(shouldBeTerminatedProbe, 6.seconds)
      probe.watch(shouldBeTerminatedProbe.ref)

      producer.terminate()
      probe.expectTerminated(shouldBeTerminatedProbe.ref)
    }

    "Check with the underlying actor if an aggregate is up to date in the KTable or not" in {
      val probe = TestProbe()
      val producer = producerMock(probe, 6.seconds)

      val aggId1 = "testAggId1"
      val futureResponse1 = producer.isAggregateStateCurrent(aggId1)
      val receivedMsg1 = probe.expectMsgType[KafkaProducerActorImpl.IsAggregateStateCurrent]
      receivedMsg1.aggregateId shouldEqual aggId1
      probe.reply(true)
      futureResponse1.futureValue shouldEqual true

      val aggId2 = "testAggId2"
      val futureResponse2 = producer.isAggregateStateCurrent(aggId2)
      val receivedMsg2 = probe.expectMsgType[KafkaProducerActorImpl.IsAggregateStateCurrent]
      receivedMsg2.aggregateId shouldEqual aggId2
      probe.reply(false)
      futureResponse2.futureValue shouldEqual false
    }

    "Ask the underlying actor if it's healthy when performing a health check" in {
      val probe = TestProbe()
      val producer = producerMock(probe, 6.seconds)

      val expectedHealthCheck = HealthCheck("test-health-check", "health-check-id", HealthCheckStatus.UP)
      val futureResult = producer.healthCheck()
      probe.expectMsg(HealthyActor.GetHealth)
      probe.reply(expectedHealthCheck)
      futureResult.futureValue shouldEqual expectedHealthCheck
    }

    "Report unhealthy if theres an error getting health from the underlying actor" in {
      val probe = TestProbe()
      val producer = producerMock(probe, 6.seconds)

      val futureResult = producer.healthCheck()
      probe.expectMsg(HealthyActor.GetHealth)
      probe.reply(Failure(new RuntimeException("This is expected")))
      futureResult.futureValue.status shouldEqual HealthCheckStatus.DOWN
    }

    "Return a failed future when the ask to the underlying publisher actor times out" in {
      implicit val ec: ExecutionContext = ExecutionContext.global
      val probe = TestProbe()
      val producer = producerMock(probe, trackerTimeout = 6.seconds)

      val errorWatchProbe = TestProbe()
      val requestId = UUID.randomUUID()
      producer
        .publish(requestId, "test", Seq(testMessage))
        .map { msg =>
          fail(s"Expected a failed future but received successful future with message [$msg]")
        }
        .recover { case e =>
          errorWatchProbe.ref ! e
        }

      probe.expectMsgClass(classOf[KafkaProducerActorImpl.Publish])

      errorWatchProbe.expectMsgType[RetryAwareException](10.seconds)
    }

    "Tracker reports published when publish succeeds" in {
      implicit val ec: ExecutionContext = ExecutionContext.global
      val probe = TestProbe()
      val producer = producerMock(probe, 6.seconds)

      val errorWatchProbe = TestProbe()
      val requestId = UUID.randomUUID()

      producer.publish(requestId, "test", Seq(testMessage)).recover { case e =>
        errorWatchProbe.ref ! e
      }

      probe.expectMsgClass(classOf[KafkaProducerActorImpl.Publish])
      probe.reply(PublishSuccess(UUID.randomUUID()))
    }
  }
}
