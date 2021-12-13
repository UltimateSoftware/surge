// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.kafka

import akka.actor.{ ActorRef, ActorSystem, Props }
import akka.pattern.ask
import akka.testkit.{ TestKit, TestProbe }
import akka.util.Timeout
import com.typesafe.config.{ Config, ConfigFactory, ConfigValueFactory }
import org.apache.kafka.clients.producer.{ ProducerRecord, RecordMetadata }
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.{ AuthorizationException, ProducerFencedException }
import org.apache.kafka.common.header.internals.{ RecordHeader, RecordHeaders }
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.mockito.{ ArgumentMatchers, Mockito }
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.{ Eventually, PatienceConfiguration, ScalaFutures }
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{ Millis, Seconds, Span }
import org.scalatest.wordspec.AnyWordSpecLike
import org.scalatestplus.mockito.MockitoSugar
import surge.core.KafkaProducerActor.{ PublishFailure, PublishResult, PublishSuccess }
import surge.core.{ KafkaProducerActor, TestBoundedContext }
import surge.health.HealthSignalBusTrait
import surge.health.domain.EmittableHealthSignal
import surge.internal.akka.cluster.ActorSystemHostAwareness
import surge.internal.akka.kafka.{ KafkaConsumerPartitionAssignmentTracker, KafkaConsumerStateTrackingActor }
import surge.internal.kafka.KafkaProducerActorImpl.KTableProgressUpdate
import surge.kafka._
import surge.kafka.streams.HealthyActor.GetHealth
import surge.kafka.streams.{ ExpectedTestException, HealthCheck, HealthCheckStatus }
import surge.metrics.Metrics

import java.time.Instant
import scala.concurrent.duration._
import scala.concurrent.{ ExecutionContext, Future }
import scala.util.control.NoStackTrace

class KafkaProducerActorImplSpec
    extends TestKit(ActorSystem("KafkaProducerActorImplSpec"))
    with AnyWordSpecLike
    with Matchers
    with BeforeAndAfterAll
    with TestBoundedContext
    with MockitoSugar
    with ScalaFutures
    with Eventually
    with PatienceConfiguration
    with ActorSystemHostAwareness {

  override val actorSystem: ActorSystem = system

  private implicit val actorAskTimeout: Timeout = Timeout(10.seconds)
  override implicit val patienceConfig: PatienceConfig = PatienceConfig(timeout = Span(3, Seconds), interval = Span(10, Millis))

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system, verifySystemShutdown = true)
  }

  private def createRecordMeta(topic: String, partition: Int, offset: Int): RecordMetadata = {
    new RecordMetadata(new TopicPartition(topic, partition), 0, offset, Instant.now.toEpochMilli, 0L, 0, 0)
  }

  private def mockKTableLagChecker(assignedPartition: TopicPartition, currentOffset: Long, endOffset: Long): KTableLagChecker = {
    val mockLagChecker = mock[KTableLagChecker]
    when(mockLagChecker.getConsumerGroupLag(ArgumentMatchers.eq(assignedPartition))).thenReturn(Some(LagInfo(currentOffset, endOffset)))

    mockLagChecker
  }

  private val defaultMockPartitionTracker: KafkaConsumerPartitionAssignmentTracker = {
    val tracker = mock[KafkaConsumerPartitionAssignmentTracker]
    when(tracker.getPartitionAssignments(any[Timeout])).thenReturn(Future.successful(PartitionAssignments(Map.empty)))
    tracker
  }

  private def testProducerActor(
      assignedPartition: TopicPartition,
      mockProducer: KafkaProducerTrait[String, Array[Byte]],
      mockLagChecker: KTableLagChecker,
      mockPartitionTracker: KafkaConsumerPartitionAssignmentTracker = defaultMockPartitionTracker,
      config: Config = ConfigFactory.load()): ActorRef = {
    val signalBus: HealthSignalBusTrait = Mockito.mock[HealthSignalBusTrait](classOf[HealthSignalBusTrait])
    val mockEmittable: EmittableHealthSignal = Mockito.mock[EmittableHealthSignal](classOf[EmittableHealthSignal])
    Mockito.when(mockEmittable.emit()).thenReturn(mockEmittable)
    Mockito.when(mockEmittable.logAsError(ArgumentMatchers.any(classOf[Option[Throwable]]))).thenReturn(mockEmittable)

    Mockito
      .when(
        signalBus.signalWithError(
          ArgumentMatchers.any(classOf[String]),
          ArgumentMatchers.any(classOf[surge.health.domain.Error]),
          ArgumentMatchers.any(classOf[Map[String, String]])))
      .thenReturn(mockEmittable)

    // Particular offset doesn't actually matter, we just want no lag
    val mockMetadata = mockRecordMetadata(assignedPartition)
    when(mockProducer.putRecord(any[ProducerRecord[String, Array[Byte]]])).thenReturn(Future.successful(mockMetadata))

    val actor =
      system.actorOf(
        Props(
          new KafkaProducerActorImpl(
            assignedPartition,
            Metrics.globalMetricRegistry,
            businessLogic,
            mockLagChecker,
            mockPartitionTracker,
            signalBus,
            config,
            Some(mockProducer))))
    // Blocks the execution to wait until the actor is ready so we know its subscribed to the event bus
    system.actorSelection(actor.path).resolveOne()(Timeout(patienceConfig.timeout)).futureValue
    actor
  }

  private def mockRecordMetadata(assignedPartition: TopicPartition): KafkaRecordMetadata[String] = {
    val recordMeta = createRecordMeta(assignedPartition.topic(), assignedPartition.partition(), 1)
    new KafkaRecordMetadata[String](None, recordMeta)
  }

  private def testObjects(strings: Seq[String]): Seq[KafkaProducerActor.MessageToPublish] = {
    strings.map { str =>
      KafkaProducerActor.MessageToPublish(str, str.getBytes(), new RecordHeaders().add(new RecordHeader("object_name", str.getBytes())))
    }
  }
  private def records(
      assignedPartition: TopicPartition,
      events: Seq[KafkaProducerActor.MessageToPublish],
      state: KafkaProducerActor.MessageToPublish): Seq[ProducerRecord[String, Array[Byte]]] = {
    val eventRecords = events.map { event =>
      new ProducerRecord(businessLogic.kafka.eventsTopic.name, null, event.key, event.value, event.headers) // scalastyle:ignore null
    }
    val stateRecord = new ProducerRecord(businessLogic.kafka.stateTopic.name, assignedPartition.partition(), state.key, state.value, state.headers)

    eventRecords :+ stateRecord
  }

  private def setupTransactions(mockProducer: KafkaProducerTrait[String, Array[Byte]]): Unit = {
    when(mockProducer.initTransactions()(any[ExecutionContext])).thenReturn(Future.unit)
    doNothing().when(mockProducer).beginTransaction()
    doNothing().when(mockProducer).abortTransaction()
    doNothing().when(mockProducer).commitTransaction()
  }

  private val illegalStateException = new IllegalStateException("This is expected") with NoStackTrace
  private val runtimeException = new RuntimeException("This is expected") with NoStackTrace

  "KafkaProducerActorImpl" should {
    val testEvents1 = testObjects(Seq("event1", "event2", "event3"))
    val testAggs1 = KafkaProducerActor.MessageToPublish("agg1", "agg1".getBytes(), new RecordHeaders())
    val testEvents2 = testObjects(Seq("event3", "event4"))
    val testAggs2 = KafkaProducerActor.MessageToPublish("agg3", "agg3".getBytes(), new RecordHeaders())

    "Recovers from beginTransaction failure" in {
      val probe = TestProbe()
      val assignedPartition = new TopicPartition("testTopic", 1)
      val mockProducer = mock[GenericKafkaProducer[String, Array[Byte]]]
      val mockMetadata = mockRecordMetadata(assignedPartition)
      when(mockProducer.initTransactions()(any[ExecutionContext])).thenReturn(Future.unit)
      // Fail first transaction and then succeed always
      doThrow(illegalStateException).doNothing().when(mockProducer).beginTransaction()
      doNothing().when(mockProducer).abortTransaction()
      doNothing().when(mockProducer).commitTransaction()

      when(mockProducer.putRecords(any[Seq[ProducerRecord[String, Array[Byte]]]])).thenReturn(Seq(Future.successful(mockMetadata)))

      val mockLagChecker = mockKTableLagChecker(assignedPartition, 100L, 100L)
      val actor = testProducerActor(assignedPartition, mockProducer, mockLagChecker)
      expectNoMessage()
      // First time beginTransaction will fail and commitTransaction won't be executed
      probe.send(actor, KafkaProducerActorImpl.Publish(testAggs1, testEvents1))
      probe.send(actor, KafkaProducerActorImpl.FlushMessages)
      expectNoMessage()
      verify(mockProducer, times(1)).beginTransaction()
      verify(mockProducer, times(0)).commitTransaction()
      expectNoMessage()
      verify(mockProducer, times(1)).beginTransaction()
      // Actor should recover from begin transaction error and successfully commit
      probe.send(actor, KafkaProducerActorImpl.Publish(testAggs1, testEvents1))
      probe.send(actor, KafkaProducerActorImpl.FlushMessages)
      expectNoMessage()
      verify(mockProducer, times(2)).beginTransaction()
      verify(mockProducer, times(1)).commitTransaction()
    }

    "Recovers from abortTransaction failure" in {
      val probe = TestProbe()
      val assignedPartition = new TopicPartition("testTopic", 1)
      val mockProducer = mock[GenericKafkaProducer[String, Array[Byte]]]
      val mockMetadata = mockRecordMetadata(assignedPartition)
      when(mockProducer.initTransactions()(any[ExecutionContext])).thenReturn(Future.unit)
      // Fail first transaction and then succeed always
      doNothing().when(mockProducer).beginTransaction()
      doNothing().when(mockProducer).close()
      doThrow(illegalStateException).when(mockProducer).abortTransaction()
      doThrow(illegalStateException).doNothing().when(mockProducer).commitTransaction()

      when(mockProducer.putRecords(any[Seq[ProducerRecord[String, Array[Byte]]]])).thenReturn(Seq(Future.successful(mockMetadata)))

      val mockLagChecker = mockKTableLagChecker(assignedPartition, 100L, 100L)
      val actor = testProducerActor(assignedPartition, mockProducer, mockLagChecker)
      expectNoMessage()
      // First time beginTransaction will fail and commitTransaction won't be executed
      probe.send(actor, KafkaProducerActorImpl.Publish(testAggs1, testEvents1))
      probe.send(actor, KafkaProducerActorImpl.FlushMessages)
      expectNoMessage()
      verify(mockProducer, times(1)).beginTransaction()
      verify(mockProducer, times(1)).commitTransaction()
      verify(mockProducer, times(1)).abortTransaction()
      verify(mockProducer, times(1)).close()
      probe.send(actor, KafkaProducerActorImpl.Publish(testAggs1, testEvents1))
      probe.send(actor, KafkaProducerActorImpl.FlushMessages)
      expectNoMessage()
      verify(mockProducer, times(2)).beginTransaction()
      verify(mockProducer, times(2)).commitTransaction()
    }

    "Gets to initialize the state if initializing kafka transactions fails with any error" in {
      val probe = TestProbe()
      val assignedPartition = new TopicPartition("testTopic", 1)
      val mockProducer = mock[GenericKafkaProducer[String, Array[Byte]]]
      val mockMetadata = mockRecordMetadata(assignedPartition)

      when(mockProducer.initTransactions()(any[ExecutionContext]))
        .thenReturn(Future.failed(new AuthorizationException("This is expected")))
        .thenReturn(Future.failed(illegalStateException))
        .thenReturn(Future.unit)
      when(mockProducer.putRecords(any[Seq[ProducerRecord[String, Array[Byte]]]])).thenReturn(Seq(Future.successful(mockMetadata)))

      val configOverride =
        ConfigFactory.load().withValue("kafka.publisher.init-transactions.authz-exception-retry-time", ConfigValueFactory.fromAnyRef("2 seconds"))
      val mockLagChecker = mockKTableLagChecker(assignedPartition, 100L, 100L)
      val actor = testProducerActor(assignedPartition, mockProducer, mockLagChecker, config = configOverride)
      probe.send(actor, KafkaProducerActorImpl.Publish(testAggs1, testEvents1))
      probe.send(actor, KafkaProducerActorImpl.FlushMessages)

      awaitAssert(
        {
          verify(mockProducer, times(3)).initTransactions()(any[ExecutionContext])
          verify(mockProducer, times(1)).beginTransaction()
          verify(mockProducer, times(1)).commitTransaction()
        },
        11.seconds,
        10.seconds)
    }

    "Not crash on failure to check KTable consumer lag" in {
      val probe = TestProbe()
      val assignedPartition = new TopicPartition("testTopic", 1)
      val mockProducer = mock[GenericKafkaProducer[String, Array[Byte]]]
      val mockMetadata = mockRecordMetadata(assignedPartition)

      when(mockProducer.initTransactions()(any[ExecutionContext])).thenReturn(Future.unit)
      when(mockProducer.putRecords(any[Seq[ProducerRecord[String, Array[Byte]]]])).thenReturn(Seq(Future.successful(mockMetadata)))

      var exceptionCount = 0
      val failsTwiceLagChecker = new KTableLagChecker {
        override def getConsumerGroupLag(assignedPartition: TopicPartition): Option[LagInfo] = {
          if (exceptionCount < 2) {
            exceptionCount = exceptionCount + 1
            throw new ExpectedTestException
          }
          Some(LagInfo(10, 10))
        }
      }
      val actor = testProducerActor(assignedPartition, mockProducer, failsTwiceLagChecker)
      probe.send(actor, KafkaProducerActorImpl.Publish(testAggs1, testEvents1))
      probe.send(actor, KafkaProducerActorImpl.FlushMessages)

      awaitAssert(
        {
          verify(mockProducer, times(1)).initTransactions()(any[ExecutionContext])
          verify(mockProducer, times(1)).beginTransaction()
          verify(mockProducer, times(1)).commitTransaction()
        },
        11.seconds,
        10.seconds)
    }

    "Reply with false to IsAggregateStateCurrent messages when uninitialized" in {
      val probe = TestProbe()
      val assignedPartition = new TopicPartition("testTopic", 1)
      val mockProducer = mock[GenericKafkaProducer[String, Array[Byte]]]

      // Always fail initTransactions() so we never make it out of the uninitialized() state
      when(mockProducer.initTransactions()(any[ExecutionContext])).thenReturn(Future.failed(illegalStateException))

      val mockLagChecker = mock[KTableLagChecker]
      when(mockLagChecker.getConsumerGroupLag(ArgumentMatchers.eq(assignedPartition))).thenReturn(Some(LagInfo(0, 10)))

      val actor = testProducerActor(assignedPartition, mockProducer, mockLagChecker)
      probe.send(actor, KafkaProducerActorImpl.IsAggregateStateCurrent("bar"))
      probe.expectMsg(false)

      probe.send(actor, GetHealth)
      val healthCheck = probe.expectMsgType[HealthCheck]
      healthCheck.status shouldEqual HealthCheckStatus.UP
      healthCheck.details should not be empty
      healthCheck.details.get.get("state") shouldEqual Some("uninitialized")
    }

    "Reply with false to IsAggregateStateCurrent messages when waiting for KTable indexing" in {
      val probe = TestProbe()
      val assignedPartition = new TopicPartition("testTopic", 1)
      val mockProducer = mock[GenericKafkaProducer[String, Array[Byte]]]
      setupTransactions(mockProducer)

      val mockLagChecker = mock[KTableLagChecker]
      when(mockLagChecker.getConsumerGroupLag(ArgumentMatchers.eq(assignedPartition))).thenReturn(Some(LagInfo(0, 10)))

      val actor = testProducerActor(assignedPartition, mockProducer, mockLagChecker)
      eventually {
        probe.send(actor, GetHealth)
        val healthCheck = probe.expectMsgType[HealthCheck]
        healthCheck.status shouldEqual HealthCheckStatus.UP
        healthCheck.details should not be empty
        healthCheck.details.get.get("state") shouldEqual Some("waitingForKTableIndexing")
      }

      probe.send(actor, KafkaProducerActorImpl.IsAggregateStateCurrent("bar"))
      probe.expectMsg(false)
    }

    "Determine if an aggregate state is up to date in the KTable based on recently published messages" in {
      val probe = TestProbe()
      val assignedPartition = new TopicPartition("testTopic", 1)
      val mockProducer = mock[GenericKafkaProducer[String, Array[Byte]]]
      val mockMetadata = mockRecordMetadata(assignedPartition)
      setupTransactions(mockProducer)
      when(mockProducer.putRecords(any[Seq[ProducerRecord[String, Array[Byte]]]])).thenReturn(Seq(Future.successful(mockMetadata)))
      val mockLagChecker = mockKTableLagChecker(assignedPartition, 100L, 100L)
      val actor = testProducerActor(assignedPartition, mockProducer, mockLagChecker)
      probe.send(actor, KafkaProducerActorImpl.Publish(testAggs1, testEvents1))
      // Wait until published message is committed to be sure we are processing
      awaitAssert(
        {
          verify(mockProducer, times(1)).commitTransaction()
        },
        10.seconds,
        1.second)

      val barRecord1 = KafkaRecordMetadata(Some("bar"), createRecordMeta("testTopic", 0, 101))
      probe.send(actor, KafkaProducerActorImpl.EventsPublished(Seq(probe.ref), Seq(barRecord1)))
      val isAggregateStateCurrent = KafkaProducerActorImpl.IsAggregateStateCurrent("bar")
      actor.ask(isAggregateStateCurrent).futureValue shouldEqual false
      // Simulate the KTable processing the recently published message
      probe.send(actor, KTableProgressUpdate(assignedPartition, LagInfo(101, 101)))
      actor.ask(isAggregateStateCurrent).futureValue shouldEqual true
    }

    "Stash Publish messages and publish them when fully initialized" in {
      val probe = TestProbe()
      val assignedPartition = new TopicPartition("testTopic", 1)
      val mockProducer = mock[GenericKafkaProducer[String, Array[Byte]]]
      val mockMetadata = mockRecordMetadata(assignedPartition)
      setupTransactions(mockProducer)
      when(mockProducer.putRecords(any[Seq[ProducerRecord[String, Array[Byte]]]])).thenReturn(Seq(Future.successful(mockMetadata)))
      val mockLagChecker = mockKTableLagChecker(assignedPartition, 100L, 100L)
      val actor = testProducerActor(assignedPartition, mockProducer, mockLagChecker)
      // Send publish messages to stash
      probe.send(actor, KafkaProducerActorImpl.Publish(testAggs1, testEvents1))
      // Verify that we haven't initialized transactions yet so we are in the uninitialized state and messages were stashed
      verify(mockProducer, times(0)).initTransactions()(any[ExecutionContext])
      // Wait until the stashed messages gets committed
      awaitAssert(
        {
          verify(mockProducer, times(1)).commitTransaction()
        },
        10.seconds,
        1.second)
    }

    "Try to publish new incoming messages to Kafka if publishing to Kafka fails" in {
      val probe = TestProbe()
      val assignedPartition = new TopicPartition("testTopic", 1)
      val mockProducerFailsPutRecords = mock[GenericKafkaProducer[String, Array[Byte]]]
      val mockLagChecker = mockKTableLagChecker(assignedPartition, 100L, 100L)
      val failingPut = testProducerActor(assignedPartition, mockProducerFailsPutRecords, mockLagChecker)

      val mockMetadata = mockRecordMetadata(assignedPartition)
      setupTransactions(mockProducerFailsPutRecords)
      when(mockProducerFailsPutRecords.putRecords(any[Seq[ProducerRecord[String, Array[Byte]]]]))
        .thenReturn(Seq(Future.failed(runtimeException)), Seq(Future.successful(mockMetadata)))

      probe.send(failingPut, KafkaProducerActorImpl.Publish(testAggs1, testEvents1))
      probe.send(failingPut, KafkaProducerActorImpl.FlushMessages)
      probe.expectMsgType[PublishFailure]
      verify(mockProducerFailsPutRecords).beginTransaction()
      verify(mockProducerFailsPutRecords).putRecords(records(assignedPartition, testEvents1, testAggs1))
      verify(mockProducerFailsPutRecords).abortTransaction()

      probe.send(failingPut, KafkaProducerActorImpl.Publish(testAggs2, testEvents2))
      probe.send(failingPut, KafkaProducerActorImpl.FlushMessages)
      probe.expectMsg(PublishSuccess)
      verify(mockProducerFailsPutRecords).putRecords(records(assignedPartition, testEvents2, testAggs2))
      verify(mockProducerFailsPutRecords).commitTransaction()
    }

    "Try to publish new incoming messages to Kafka if committing to Kafka fails" in {
      val probe = TestProbe()
      val assignedPartition = new TopicPartition("testTopic", 1)
      val mockProducerFailsCommit = mock[GenericKafkaProducer[String, Array[Byte]]]
      val mockLagChecker = mockKTableLagChecker(assignedPartition, 100L, 100L)
      val failingCommit = testProducerActor(assignedPartition, mockProducerFailsCommit, mockLagChecker)

      when(mockProducerFailsCommit.initTransactions()(any[ExecutionContext])).thenReturn(Future.unit)
      doNothing().when(mockProducerFailsCommit).beginTransaction()
      doNothing().when(mockProducerFailsCommit).abortTransaction()
      when(mockProducerFailsCommit.commitTransaction()).thenThrow(runtimeException)

      val mockMetadata = mockRecordMetadata(assignedPartition)
      when(mockProducerFailsCommit.putRecords(any[Seq[ProducerRecord[String, Array[Byte]]]])).thenReturn(Seq(Future.successful(mockMetadata)))

      probe.send(failingCommit, KafkaProducerActorImpl.Publish(testAggs1, testEvents1))
      probe.send(failingCommit, KafkaProducerActorImpl.FlushMessages)
      probe.expectMsgType[PublishFailure]
      verify(mockProducerFailsCommit).beginTransaction()
      verify(mockProducerFailsCommit).putRecords(records(assignedPartition, testEvents1, testAggs1))
      verify(mockProducerFailsCommit).commitTransaction()
      verify(mockProducerFailsCommit).abortTransaction()

      // Since we can't stub the void method to throw an exception and then succeed, we just care that the actor attempts to send the next set of records
      probe.send(failingCommit, KafkaProducerActorImpl.Publish(testAggs2, testEvents2))
      probe.send(failingCommit, KafkaProducerActorImpl.FlushMessages)
      probe.expectMsgType[PublishFailure]
      verify(mockProducerFailsCommit).putRecords(records(assignedPartition, testEvents2, testAggs2))
    }

    "Stop if the producer is fenced out by another instance with the same transaction id" in {
      val probe = TestProbe()
      val partitionTrackerProbe = TestProbe()
      val terminateWatcherProbe = TestProbe()
      val assignedPartition = new TopicPartition("testTopic", 1)
      val mockProducerFenceOnBegin = mock[GenericKafkaProducer[String, Array[Byte]]]
      val mockProducerFenceOnCommit = mock[GenericKafkaProducer[String, Array[Byte]]]
      val mockMetadata = mockRecordMetadata(assignedPartition)
      val mockPartitionTracker = new KafkaConsumerPartitionAssignmentTracker(partitionTrackerProbe.ref)

      when(mockProducerFenceOnBegin.initTransactions()(any[ExecutionContext])).thenReturn(Future.unit)
      doThrow(new ProducerFencedException("This is expected")).when(mockProducerFenceOnBegin).beginTransaction()
      doNothing().when(mockProducerFenceOnBegin).abortTransaction()
      doNothing().when(mockProducerFenceOnBegin).commitTransaction()
      when(mockProducerFenceOnBegin.putRecords(any[Seq[ProducerRecord[String, Array[Byte]]]])).thenReturn(Seq(Future.successful(mockMetadata)))

      when(mockProducerFenceOnCommit.initTransactions()(any[ExecutionContext])).thenReturn(Future.unit)
      doNothing().when(mockProducerFenceOnCommit).beginTransaction()
      doNothing().when(mockProducerFenceOnCommit).abortTransaction()
      doThrow(new ProducerFencedException("This is expected")).when(mockProducerFenceOnCommit).commitTransaction()
      when(mockProducerFenceOnCommit.putRecords(any[Seq[ProducerRecord[String, Array[Byte]]]])).thenReturn(Seq(Future.successful(mockMetadata)))

      val fastHealthCheckFailureConfig =
        ConfigFactory.parseString("kafka.publisher.fenced-unhealthy-report-time = 0 milliseconds").withFallback(ConfigFactory.load())
      val mockLagChecker = mockKTableLagChecker(assignedPartition, 100L, 100L)
      val fencedOnBegin = testProducerActor(assignedPartition, mockProducerFenceOnBegin, mockLagChecker, mockPartitionTracker)
      val fencedOnCommit = testProducerActor(assignedPartition, mockProducerFenceOnCommit, mockLagChecker, mockPartitionTracker, fastHealthCheckFailureConfig)

      terminateWatcherProbe.watch(fencedOnBegin)
      probe.send(fencedOnBegin, KafkaProducerActorImpl.Publish(testAggs1, testEvents1))
      probe.send(fencedOnBegin, KafkaProducerActorImpl.FlushMessages)
      probe.expectMsgType[PublishFailure]

      partitionTrackerProbe.expectMsg(KafkaConsumerStateTrackingActor.GetPartitionAssignments)
      probe.send(fencedOnBegin, GetHealth)
      val shouldBeUpHealthCheck = probe.expectMsgType[HealthCheck]
      shouldBeUpHealthCheck.status shouldEqual HealthCheckStatus.UP
      partitionTrackerProbe.reply(PartitionAssignments(Map.empty))
      terminateWatcherProbe.expectTerminated(fencedOnBegin)

      verify(mockProducerFenceOnBegin).beginTransaction()
      verify(mockProducerFenceOnBegin, times(0)).putRecords(records(assignedPartition, testEvents1, testAggs1))

      terminateWatcherProbe.watch(fencedOnCommit)
      probe.send(fencedOnCommit, KafkaProducerActorImpl.Publish(testAggs1, testEvents1))
      probe.send(fencedOnCommit, KafkaProducerActorImpl.FlushMessages)
      probe.expectMsgType[PublishFailure]

      partitionTrackerProbe.expectMsg(KafkaConsumerStateTrackingActor.GetPartitionAssignments)
      probe.send(fencedOnCommit, GetHealth)
      val shouldBeDownHealthCheck = probe.expectMsgType[HealthCheck]
      shouldBeDownHealthCheck.status shouldEqual HealthCheckStatus.DOWN
      partitionTrackerProbe.reply(PartitionAssignments(Map.empty))
      terminateWatcherProbe.expectTerminated(fencedOnCommit)

      verify(mockProducerFenceOnCommit).beginTransaction()
      verify(mockProducerFenceOnCommit).putRecords(records(assignedPartition, testEvents1, testAggs1))
      verify(mockProducerFenceOnCommit).commitTransaction()
    }

    "Recreate the producer on a ProducerFencedException if the partition is still assigned to this node" in {
      val probe = TestProbe()
      val assignedPartition = new TopicPartition("testTopic", 1)
      val mockPartitionTracker = mock[KafkaConsumerPartitionAssignmentTracker]
      val assignmentMap = Map(localHostPort -> List(assignedPartition))
      val mockProducerFenceOnCommit = mock[GenericKafkaProducer[String, Array[Byte]]]
      val mockMetadata = mockRecordMetadata(assignedPartition)

      when(mockPartitionTracker.getPartitionAssignments(any[Timeout])).thenReturn(Future.successful(PartitionAssignments(assignmentMap)))
      when(mockProducerFenceOnCommit.initTransactions()(any[ExecutionContext])).thenReturn(Future.unit)
      doNothing().when(mockProducerFenceOnCommit).beginTransaction()
      doNothing().when(mockProducerFenceOnCommit).abortTransaction()
      doThrow(new ProducerFencedException("This is expected")).doNothing().when(mockProducerFenceOnCommit).commitTransaction()
      when(mockProducerFenceOnCommit.putRecords(any[Seq[ProducerRecord[String, Array[Byte]]]])).thenReturn(Seq(Future.successful(mockMetadata)))

      val mockLagChecker = mockKTableLagChecker(assignedPartition, 100L, 100L)
      val fencedOnCommit = testProducerActor(assignedPartition, mockProducerFenceOnCommit, mockLagChecker, mockPartitionTracker)

      probe.watch(fencedOnCommit)
      probe.send(fencedOnCommit, KafkaProducerActorImpl.Publish(testAggs1, testEvents1))
      probe.send(fencedOnCommit, KafkaProducerActorImpl.FlushMessages)
      val result = probe.fishForMessage(10.seconds) { msg => msg.isInstanceOf[PublishResult] }
      result shouldBe a[PublishFailure]

      verify(mockProducerFenceOnCommit).beginTransaction()
      verify(mockProducerFenceOnCommit).putRecords(records(assignedPartition, testEvents1, testAggs1))
      verify(mockProducerFenceOnCommit).commitTransaction()

      probe.send(fencedOnCommit, KafkaProducerActorImpl.Publish(testAggs2, testEvents2))
      probe.send(fencedOnCommit, KafkaProducerActorImpl.FlushMessages)
      probe.expectMsg(PublishSuccess)
      verify(mockProducerFenceOnCommit).putRecords(records(assignedPartition, testEvents2, testAggs2))
    }
  }

  "KafkaProducerActorState" should {
    val fooRecord1 = KafkaRecordMetadata(Some("foo"), createRecordMeta("testTopic", 0, 1))
    val barRecord1 = KafkaRecordMetadata(Some("bar"), createRecordMeta("testTopic", 0, 2))
    val bazRecord1 = KafkaRecordMetadata(Some("baz"), createRecordMeta("testTopic", 0, 3))
    val fooRecord2 = KafkaRecordMetadata(Some("foo"), createRecordMeta("testTopic", 0, 4))
    val barRecord2 = KafkaRecordMetadata(Some("bar"), createRecordMeta("testTopic", 0, 5))

    val exampleMetadata: Seq[KafkaRecordMetadata[String]] = Seq(fooRecord1, barRecord1, bazRecord1, fooRecord2)

    "Record new record metadata marked as in flight" in {
      val empty = KafkaProducerActorState.empty

      val newState = empty.addInFlight(exampleMetadata)
      newState.inFlight should contain allElementsOf Seq(barRecord1, bazRecord1, fooRecord2)

      newState.inFlightForAggregate("foo") should contain only fooRecord2
      newState.inFlightForAggregate("bar") should contain only barRecord1
      newState.inFlightForAggregate("baz") should contain only bazRecord1
      (newState.inFlightForAggregate("missing") should have).length(0)

      val newState2 = newState.addInFlight(Seq(barRecord2))
      newState2.inFlightForAggregate("bar") should contain only barRecord2
    }

    "Track buffered pending writes" in {
      val empty = KafkaProducerActorState.empty

      val sender = TestProbe().ref
      val dummyState = KafkaProducerActor.MessageToPublish("foo", "foo".getBytes(), new RecordHeaders())
      val publishMsg = KafkaProducerActorImpl.Publish(dummyState, Seq(KafkaProducerActor.MessageToPublish("foo", "foo".getBytes(), new RecordHeaders())))

      val newState = empty.addPendingWrites(sender, publishMsg)
      newState.pendingWrites should contain only KafkaProducerActorImpl.PublishWithSender(sender, publishMsg)

      val flushedState = newState.flushWrites()
      (flushedState.pendingWrites should have).length(0)
    }

    "Calculate how long a transaction has been in progress for" in {
      val stateWithNoTransaction = KafkaProducerActorState.empty
      stateWithNoTransaction.currentTransactionTimeMillis shouldEqual 0L

      val expectedTransactionTime = 25L
      val transactionStartTime = Instant.now.minusMillis(expectedTransactionTime)
      val stateWithActiveTransaction = stateWithNoTransaction.copy(transactionInProgressSince = Some(transactionStartTime))
      stateWithActiveTransaction.currentTransactionTimeMillis shouldEqual expectedTransactionTime +- 2 // State creates a new Instant, so approximate is fine
    }
  }
}
