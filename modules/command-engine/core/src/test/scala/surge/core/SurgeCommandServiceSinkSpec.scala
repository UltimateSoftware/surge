// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.core

import akka.actor.{ ActorSystem, NoSerializationVerificationNeeded }
import akka.testkit.{ TestKit, TestProbe }
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import surge.core.command.{ SurgeCommandServiceSink, SurgeMultiCommandServiceSink }
import surge.streams.sink.TestEventSource

import java.util.UUID
import scala.concurrent.{ ExecutionContext, Future }

class SurgeCommandServiceSinkSpec extends TestKit(ActorSystem("SurgeCommandServiceSinkSpec")) with AnyWordSpecLike with Matchers with BeforeAndAfterAll {
  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }

  private sealed trait TestCommand {
    def aggregateId: String
  }
  private case class Command1(aggregateId: String) extends TestCommand
  private case class Command2(aggregateId: String) extends TestCommand

  private sealed trait TestEvent {
    def toCommands: Seq[TestCommand]
  }
  private case class Event1(aggregateId: String) extends TestEvent {
    override def toCommands: Seq[TestCommand] = Seq(Command1(aggregateId))
  }
  private case class Event2(aggregateId: String) extends TestEvent {
    override def toCommands: Seq[TestCommand] = Seq(Command2(aggregateId))
  }
  private case class EventWithNoCommand() extends TestEvent {
    override def toCommands: Seq[TestCommand] = Seq.empty
  }
  private case class EventWithMultiCommand(aggregateId: String) extends TestEvent {
    override def toCommands: Seq[TestCommand] = Seq(Command1(aggregateId), Command2(aggregateId))
  }
  private case class ReceivedMsg(aggId: String, command: TestCommand) extends NoSerializationVerificationNeeded
  private def testSink(probe: TestProbe): SurgeCommandServiceSink[String, TestCommand, TestEvent] = {
    new SurgeCommandServiceSink[String, TestCommand, TestEvent] {
      override def eventToCommand: TestEvent => Option[TestCommand] = _.toCommands.headOption
      override implicit def executionContext: ExecutionContext = ExecutionContext.global
      override protected def sendToAggregate(aggId: String, command: TestCommand): Future[Any] = {
        probe.ref ! ReceivedMsg(aggId, command)
        Future.successful(true)
      }
      override def aggregateIdFromCommand: TestCommand => String = _.aggregateId
      override def partitionBy(key: String, event: TestEvent, headers: Map[String, Array[Byte]]): String = ""
    }
  }
  private def testMultiSink(probe: TestProbe): SurgeMultiCommandServiceSink[String, TestCommand, TestEvent] = {
    new SurgeMultiCommandServiceSink[String, TestCommand, TestEvent] {
      override def eventToCommands: TestEvent => Seq[TestCommand] = _.toCommands
      override implicit def executionContext: ExecutionContext = ExecutionContext.global
      override protected def sendToAggregate(aggId: String, command: TestCommand): Future[Any] = {
        probe.ref ! ReceivedMsg(aggId, command)
        Future.successful(true)
      }
      override def aggregateIdFromCommand: TestCommand => String = _.aggregateId
      override def partitionBy(key: String, event: TestEvent, headers: Map[String, Array[Byte]]): String = ""
    }
  }
  "SurgeCommandServiceSink" should {
    "Forward commands to the appropriate aggregate" in {
      val testAggId1 = UUID.randomUUID().toString
      val testAggId2 = UUID.randomUUID().toString
      val testProbe = TestProbe()
      val controlPipeline = new TestEventSource[TestEvent]().to(testSink(testProbe), "example-consumer-group")

      controlPipeline.sendEvent(Event1(testAggId1))
      testProbe.expectMsg(ReceivedMsg(testAggId1, Command1(testAggId1)))
      controlPipeline.sendEvent(Event2(testAggId2))
      testProbe.expectMsg(ReceivedMsg(testAggId2, Command2(testAggId2)))
    }

    "Not forward anything if no command is generated" in {
      val testProbe = TestProbe()
      val controlPipeline = new TestEventSource[TestEvent]().to(testSink(testProbe), "example-consumer-group")
      controlPipeline.sendEvent(EventWithNoCommand())
      testProbe.expectNoMessage()
    }
  }

  "SurgeMultiCommandServiceSink" should {
    "Forward multiple commands to the appropriate aggregate" in {
      val testAggId = UUID.randomUUID().toString
      val testProbe = TestProbe()
      val controlPipeline = new TestEventSource[TestEvent]().to(testMultiSink(testProbe), "example-consumer-group")

      controlPipeline.sendEvent(EventWithMultiCommand(testAggId))
      testProbe.expectMsg(ReceivedMsg(testAggId, Command1(testAggId)))
      testProbe.expectMsg(ReceivedMsg(testAggId, Command2(testAggId)))
    }
  }
}
