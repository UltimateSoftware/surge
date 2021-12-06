// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.core

import akka.actor.{ ActorRef, ActorSystem, Props }
import akka.testkit.{ TestKit, TestProbe }
import io.opentelemetry.api.trace.Tracer
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpecLike
import surge.exceptions.SurgeUnexpectedException
import surge.internal.persistence.{ AggregateRefTrait, PersistentActor }
import surge.internal.tracing.{ NoopTracerFactory, ProbeWithTraceSupport }

import scala.concurrent.Future

class AggregateRefTraitSpec
    extends TestKit(ActorSystem("AggregateRefTraitSpec"))
    with AsyncWordSpecLike
    with Matchers
    with ScalaFutures
    with BeforeAndAfterAll {

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system, verifySystemShutdown = true)
  }

  case class Person(name: String, favoriteColor: String)

  private val noopTracer = NoopTracerFactory.create()

  case class TestAggregateRef(aggregateId: String, regionTestProbe: TestProbe) extends AggregateRefTrait[String, Person, String, String] {
    override val region: ActorRef = system.actorOf(Props(new ProbeWithTraceSupport(regionTestProbe, noopTracer)))
    override val tracer: Tracer = noopTracer

    def sendCommand(command: String): Future[Option[Person]] = {
      val envelope = PersistentActor.ProcessMessage(aggregateId, command)
      sendCommand(envelope)
    }

    def applyEvents(events: List[String], retries: Int = 0): Future[Option[Person]] = {
      val envelope = PersistentActor.ApplyEvents(aggregateId, events)
      applyEvents(envelope)
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
      testProbe1.expectMsg(PersistentActor.GetState(testPerson1.name))
      testProbe1.reply(PersistentActor.StateResponse(Some(testPerson1)))

      val testPerson2StateFut = aggregateRef2.getState
      testProbe2.expectMsg(PersistentActor.GetState(testPerson2.name))
      testProbe2.reply(PersistentActor.StateResponse(None))

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
      testProbe1.expectMsg(PersistentActor.ProcessMessage(testPerson1.name, "Command1"))
      testProbe1.reply(PersistentActor.ACKSuccess(Some(testPerson1)))

      val testProbe2 = TestProbe()
      val aggregateRef2 = TestAggregateRef(testPerson2.name, testProbe2)
      val testPerson2StateFut = aggregateRef2.sendCommand("Command2")
      testProbe2.expectMsg(PersistentActor.ProcessMessage(testPerson2.name, "Command2"))
      testProbe2.reply(PersistentActor.ACKSuccess(None))

      val errorProbe = TestProbe()
      val expectedException = new RuntimeException("This is expected")
      val testErrorResponseFut = TestAggregateRef("error", errorProbe).sendCommand("Command4")
      errorProbe.expectMsg(PersistentActor.ProcessMessage("error", "Command4"))
      errorProbe.reply(PersistentActor.ACKError(expectedException))

      val garbageProbe = TestProbe()
      val testGarbageResponseFut = TestAggregateRef("foo", garbageProbe).sendCommand("Command5")
      garbageProbe.expectMsg(PersistentActor.ProcessMessage("foo", "Command5"))
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

    "Handle applying events to the underlying aggregate" in {
      val testProbe1 = TestProbe()
      val aggregateRef1 = TestAggregateRef(testPerson1.name, testProbe1)
      val testPerson1StateFut = aggregateRef1.applyEvents(List("Event1"))
      testProbe1.expectMsg(PersistentActor.ApplyEvents(testPerson1.name, List("Event1")))
      testProbe1.reply(PersistentActor.ACKSuccess(Some(testPerson1)))

      val testProbe2 = TestProbe()
      val aggregateRef2 = TestAggregateRef(testPerson2.name, testProbe2)
      val testPerson2StateFut = aggregateRef2.applyEvents(List("Event2"))
      testProbe2.expectMsg(PersistentActor.ApplyEvents(testPerson2.name, List("Event2")))
      testProbe2.reply(PersistentActor.ACKSuccess(None))

      val errorProbe = TestProbe()
      val expectedException = new RuntimeException("This is expected")
      val testErrorResponseFut = TestAggregateRef("error", errorProbe).applyEvents(List("Event3"))
      errorProbe.expectMsg(PersistentActor.ApplyEvents("error", List("Event3")))
      errorProbe.reply(PersistentActor.ACKError(expectedException))

      val garbageProbe = TestProbe()
      val testGarbageResponseFut = TestAggregateRef("foo", garbageProbe).applyEvents(List("Event4"))
      garbageProbe.expectMsg(PersistentActor.ApplyEvents("foo", List("Event4")))
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
