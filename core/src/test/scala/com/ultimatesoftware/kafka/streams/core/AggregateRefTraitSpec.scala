// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams.core

import akka.actor.{ ActorRef, ActorSystem }
import akka.testkit.{ TestKit, TestProbe }
import com.ultimatesoftware.scala.core.validations
import org.scalatest.{ AsyncWordSpecLike, Matchers }

import scala.concurrent.Future

class AggregateRefTraitSpec extends TestKit(ActorSystem("AggregateRefTraitSpec")) with AsyncWordSpecLike with Matchers {

  case class Person(name: String, favoriteColor: String)

  case class TestAggregateRef(aggregateId: String, regionTestProbe: TestProbe) extends AggregateRefTrait[String, Person, String, String] {
    override val region: ActorRef = regionTestProbe.ref

    def ask(command: String, retries: Int = 0): Future[Either[Seq[validations.ValidationError], Option[Person]]] = {
      val envelope = GenericAggregateActor.CommandEnvelope(aggregateId, "", command)
      askWithRetries(envelope, retries)
    }
    def getState: Future[Option[Person]] = queryState
  }
  val testPerson1 = Person("Timmy", "Red")
  val testPerson2 = Person("Joyce", "Green")
  val testPerson3 = Person("Scott", "Blue")

  "AggregateRef" should {
    "Be able to fetch state for an aggregate" in {
      val testProbe1 = TestProbe()
      val aggregateRef1 = TestAggregateRef(testPerson1.name, testProbe1)

      val testProbe2 = TestProbe()
      val aggregateRef2 = TestAggregateRef(testPerson2.name, testProbe2)

      val testPerson1StateFut = aggregateRef1.getState
      testProbe1.expectMsg(GenericAggregateActor.GetState(testPerson1.name))
      testProbe1.reply(Some(testPerson1))

      val testPerson2StateFut = aggregateRef2.getState
      testProbe2.expectMsg(GenericAggregateActor.GetState(testPerson2.name))
      testProbe2.reply(None)

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
      testProbe1.expectMsg(GenericAggregateActor.CommandEnvelope(testPerson1.name, "", "Command1"))
      testProbe1.reply(GenericAggregateActor.CommandSuccess(Some(testPerson1)))

      val testProbe2 = TestProbe()
      val aggregateRef2 = TestAggregateRef(testPerson2.name, testProbe2)
      val testPerson2StateFut = aggregateRef2.ask("Command2")
      testProbe2.expectMsg(GenericAggregateActor.CommandEnvelope(testPerson2.name, "", "Command2"))
      testProbe2.reply(GenericAggregateActor.CommandSuccess(None))

      val testProbe3 = TestProbe()
      val aggregateRef3 = TestAggregateRef(testPerson3.name, testProbe3)
      val testPerson3StateFut = aggregateRef3.ask("Command3")
      testProbe3.expectMsg(GenericAggregateActor.CommandEnvelope(testPerson3.name, "", "Command3"))
      testProbe3.reply(GenericAggregateActor.CommandFailure(Seq.empty))

      val garbageProbe = TestProbe()
      val testGarbageResponseFut = TestAggregateRef("foo", garbageProbe).ask("Command4")
      garbageProbe.expectMsg(GenericAggregateActor.CommandEnvelope("foo", "", "Command4"))
      garbageProbe.reply("Not a person object")

      for {
        testPerson1State ← testPerson1StateFut
        testPerson2State ← testPerson2StateFut
        testPerson3State ← testPerson3StateFut
        testGarbageResponse ← testGarbageResponseFut
      } yield {
        testPerson1State shouldEqual Right(Some(testPerson1))
        testPerson2State shouldEqual Right(None)
        testPerson3State shouldEqual Left(Seq.empty)
        testGarbageResponse shouldEqual Right(None)
      }

    }
  }

}
