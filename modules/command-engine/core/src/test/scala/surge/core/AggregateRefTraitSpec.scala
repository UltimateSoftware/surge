// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.core

import akka.actor.{ ActorRef, ActorSystem, Props }
import akka.testkit.{ TestKit, TestProbe }
import io.opentracing.Tracer
import io.opentracing.mock.MockTracer
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpecLike
import surge.exceptions.SurgeUnexpectedException
import surge.internal.akka.ProbeWithTraceSupport
import surge.internal.persistence.cqrs.CQRSPersistentActor

import scala.concurrent.Future

class AggregateRefTraitSpec extends TestKit(ActorSystem("AggregateRefTraitSpec")) with AsyncWordSpecLike with Matchers with ScalaFutures {

  case class Person(name: String, favoriteColor: String)

  private val mockTracer = new MockTracer()
  case class TestAggregateRef(aggregateId: String, regionTestProbe: TestProbe) extends AggregateRefTrait[String, Person, String, String] {
    override val region: ActorRef = system.actorOf(Props(new ProbeWithTraceSupport(regionTestProbe, mockTracer)))
    override val tracer: Tracer = mockTracer

    def sendCommand(command: String, retries: Int = 0): Future[Either[Throwable, Option[Person]]] = {
      val envelope = CQRSPersistentActor.CommandEnvelope(aggregateId, command)
      sendCommandWithRetries(envelope, retries)
    }
    def applyEvent(event: String, retries: Int = 0): Future[Option[Person]] = {
      val envelope = CQRSPersistentActor.ApplyEventEnvelope(aggregateId, event)
      applyEventsWithRetries(envelope, retries)
    }
    def getState: Future[Option[Person]] = queryState
  }
  private val testPerson1 = Person("Timmy", "Red")
  private val testPerson2 = Person("Joyce", "Green")

  "AggregateRef" should {
    "Be able to fetch state for an aggregate" in {
      val testProbe1 = TestProbe()
      val aggregateRef1 = TestAggregateRef(testPerson1.name, testProbe1)

      val testProbe2 = TestProbe()
      val aggregateRef2 = TestAggregateRef(testPerson2.name, testProbe2)

      val testPerson1StateFut = aggregateRef1.getState
      testProbe1.expectMsg(CQRSPersistentActor.GetState(testPerson1.name))
      testProbe1.reply(CQRSPersistentActor.StateResponse(Some(testPerson1)))

      val testPerson2StateFut = aggregateRef2.getState
      testProbe2.expectMsg(CQRSPersistentActor.GetState(testPerson2.name))
      testProbe2.reply(CQRSPersistentActor.StateResponse(None))

      for {
        testPerson1State <- testPerson1StateFut
        testPerson2State <- testPerson2StateFut
      } yield {
        testPerson1State shouldEqual Some(testPerson1)
        testPerson2State shouldEqual None
      }
    }

    "Handle sending commands to the underlying aggregate" in {
      val testProbe1 = TestProbe()
      val aggregateRef1 = TestAggregateRef(testPerson1.name, testProbe1)
      val testPerson1StateFut = aggregateRef1.sendCommand("Command1")
      testProbe1.expectMsg(CQRSPersistentActor.CommandEnvelope(testPerson1.name, "Command1"))
      testProbe1.reply(CQRSPersistentActor.CommandSuccess(Some(testPerson1)))

      val testProbe2 = TestProbe()
      val aggregateRef2 = TestAggregateRef(testPerson2.name, testProbe2)
      val testPerson2StateFut = aggregateRef2.sendCommand("Command2")
      testProbe2.expectMsg(CQRSPersistentActor.CommandEnvelope(testPerson2.name, "Command2"))
      testProbe2.reply(CQRSPersistentActor.CommandSuccess(None))

      val errorProbe = TestProbe()
      val expectedException = new RuntimeException("This is expected")
      val testErrorResponseFut = TestAggregateRef("error", errorProbe).sendCommand("Command4")
      errorProbe.expectMsg(CQRSPersistentActor.CommandEnvelope("error", "Command4"))
      errorProbe.reply(CQRSPersistentActor.CommandError(expectedException))

      val garbageProbe = TestProbe()
      val testGarbageResponseFut = TestAggregateRef("foo", garbageProbe).sendCommand("Command5")
      garbageProbe.expectMsg(CQRSPersistentActor.CommandEnvelope("foo", "Command5"))
      garbageProbe.reply("Not a person object")

      for {
        testPerson1State <- testPerson1StateFut
        testPerson2State <- testPerson2StateFut
        testErrorResponse <- testErrorResponseFut
        testGarbageResponse <- testGarbageResponseFut
      } yield {
        testPerson1State shouldEqual Right(Some(testPerson1))
        testPerson2State shouldEqual Right(None)
        testErrorResponse shouldEqual Left(expectedException)
        testGarbageResponse shouldBe a[Left[Throwable, _]]
      }
    }

    "Handle applying events to the underlying aggregate" in {
      val testProbe1 = TestProbe()
      val aggregateRef1 = TestAggregateRef(testPerson1.name, testProbe1)
      val testPerson1StateFut = aggregateRef1.applyEvent("Event1")
      testProbe1.expectMsg(CQRSPersistentActor.ApplyEventEnvelope(testPerson1.name, "Event1"))
      testProbe1.reply(CQRSPersistentActor.CommandSuccess(Some(testPerson1)))

      val testProbe2 = TestProbe()
      val aggregateRef2 = TestAggregateRef(testPerson2.name, testProbe2)
      val testPerson2StateFut = aggregateRef2.applyEvent("Event2")
      testProbe2.expectMsg(CQRSPersistentActor.ApplyEventEnvelope(testPerson2.name, "Event2"))
      testProbe2.reply(CQRSPersistentActor.CommandSuccess(None))

      val errorProbe = TestProbe()
      val expectedException = new RuntimeException("This is expected")
      val testErrorResponseFut = TestAggregateRef("error", errorProbe).applyEvent("Event3")
      errorProbe.expectMsg(CQRSPersistentActor.ApplyEventEnvelope("error", "Event3"))
      errorProbe.reply(CQRSPersistentActor.CommandError(expectedException))

      val garbageProbe = TestProbe()
      val testGarbageResponseFut = TestAggregateRef("foo", garbageProbe).applyEvent("Event4")
      garbageProbe.expectMsg(CQRSPersistentActor.ApplyEventEnvelope("foo", "Event4"))
      garbageProbe.reply("Not a person object")

      for {
        testPerson1State <- testPerson1StateFut
        testPerson2State <- testPerson2StateFut
        testErrorResponse <- testErrorResponseFut.failed
        testGarbageResponse <- testGarbageResponseFut.failed
      } yield {
        testPerson1State shouldEqual Some(testPerson1)
        testPerson2State shouldEqual None
        testErrorResponse shouldEqual expectedException
        testGarbageResponse shouldBe a[SurgeUnexpectedException]
      }
    }
  }
}
