// Copyright © 2018-2020 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams.core

import akka.actor.{ ActorRef, ActorSystem }
import akka.testkit.{ TestKit, TestProbe }
import com.ultimatesoftware.scala.core.validations.ValidationError
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpecLike

import scala.concurrent.Future

class AggregateRefTraitSpec extends TestKit(ActorSystem("AggregateRefTraitSpec")) with AsyncWordSpecLike with Matchers {

  case class Person(name: String, favoriteColor: String)

  case class TestAggregateRef(aggregateId: String, regionTestProbe: TestProbe) extends AggregateRefTrait[Person, String, String] {
    override val region: ActorRef = regionTestProbe.ref

    def ask(command: String, retries: Int = 0): Future[Either[Throwable, Option[Person]]] = {
      val envelope = GenericAggregateActor.CommandEnvelope(aggregateId, command)
      askWithRetries(envelope, retries)
    }
    def getState: Future[Option[Person]] = queryState
  }
  private val testPerson1 = Person("Timmy", "Red")
  private val testPerson2 = Person("Joyce", "Green")
  private val testPerson3 = Person("Scott", "Blue")

  "AggregateRef" should {
    "Be able to fetch state for an aggregate" in {
      val testProbe1 = TestProbe()
      val aggregateRef1 = TestAggregateRef(testPerson1.name, testProbe1)

      val testProbe2 = TestProbe()
      val aggregateRef2 = TestAggregateRef(testPerson2.name, testProbe2)

      val testPerson1StateFut = aggregateRef1.getState
      testProbe1.expectMsg(GenericAggregateActor.GetState(testPerson1.name))
      testProbe1.reply(GenericAggregateActor.StateResponse(Some(testPerson1)))

      val testPerson2StateFut = aggregateRef2.getState
      testProbe2.expectMsg(GenericAggregateActor.GetState(testPerson2.name))
      testProbe2.reply(GenericAggregateActor.StateResponse(None))

      for {
        testPerson1State ← testPerson1StateFut
        testPerson2State ← testPerson2StateFut
      } yield {
        testPerson1State shouldEqual Some(testPerson1)
        testPerson2State shouldEqual None
      }
    }

    "Handle asks to the underlying aggregate" in {
      val testProbe1 = TestProbe()
      val aggregateRef1 = TestAggregateRef(testPerson1.name, testProbe1)
      val testPerson1StateFut = aggregateRef1.ask("Command1")
      testProbe1.expectMsg(GenericAggregateActor.CommandEnvelope(testPerson1.name, "Command1"))
      testProbe1.reply(GenericAggregateActor.CommandSuccess(Some(testPerson1)))

      val testProbe2 = TestProbe()
      val aggregateRef2 = TestAggregateRef(testPerson2.name, testProbe2)
      val testPerson2StateFut = aggregateRef2.ask("Command2")
      testProbe2.expectMsg(GenericAggregateActor.CommandEnvelope(testPerson2.name, "Command2"))
      testProbe2.reply(GenericAggregateActor.CommandSuccess(None))

      val failureProbe = TestProbe()
      val validationErrors = Seq(ValidationError("This is expected"))
      val aggregateRef3 = TestAggregateRef(testPerson3.name, failureProbe)
      val testFailureResponseFut = aggregateRef3.ask("Command3")
      failureProbe.expectMsg(GenericAggregateActor.CommandEnvelope(testPerson3.name, "Command3"))
      failureProbe.reply(GenericAggregateActor.CommandFailure(validationErrors))

      val errorProbe = TestProbe()
      val expectedException = new RuntimeException("This is expected")
      val testErrorResponseFut = TestAggregateRef("error", errorProbe).ask("Command4")
      errorProbe.expectMsg(GenericAggregateActor.CommandEnvelope("error", "Command4"))
      errorProbe.reply(GenericAggregateActor.CommandError(expectedException))

      val garbageProbe = TestProbe()
      val testGarbageResponseFut = TestAggregateRef("foo", garbageProbe).ask("Command5")
      garbageProbe.expectMsg(GenericAggregateActor.CommandEnvelope("foo", "Command5"))
      garbageProbe.reply("Not a person object")

      for {
        testPerson1State ← testPerson1StateFut
        testPerson2State ← testPerson2StateFut
        testFailureResponse ← testFailureResponseFut
        testErrorResponse ← testErrorResponseFut
        testGarbageResponse ← testGarbageResponseFut
      } yield {
        testPerson1State shouldEqual Right(Some(testPerson1))
        testPerson2State shouldEqual Right(None)
        testFailureResponse shouldEqual Left(DomainValidationError(validationErrors))
        testErrorResponse shouldEqual Left(expectedException)
        testGarbageResponse shouldBe a[Left[Throwable, _]]
      }

    }
  }

}
