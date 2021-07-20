// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.akka.cluster

import akka.actor.{ Actor, ActorContext, ActorRef, ActorSystem, DeadLetter, PoisonPill, Props, Terminated }
import akka.testkit.{ TestKit, TestProbe }
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import org.scalatestplus.mockito.MockitoSugar
import surge.akka.cluster.{ EntityPropsProvider, Passivate, PerShardLogicProvider }
import surge.internal.tracing.NoopTracerFactory
import surge.kafka.streams.{ HealthCheck, HealthCheckStatus }

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

object TestActor {
  def props(id: String): Props = Props(new TestActor(id))

  sealed trait Message {
    def actorIdentifier: String
  }

  case class Update(actorIdentifier: String, value: Int) extends Message

  case class Delete(actorIdentifier: String) extends Message

  case class Get(actorIdentifier: String) extends Message

  case class DoPassivate(actorIdentifier: String) extends Message

  case class StopActor(originalSender: ActorRef)

  case class Crash(actorIdentifier: String, exception: Exception) extends Message

  def idExtractor: PartialFunction[Any, String] = { case m: Message =>
    m.actorIdentifier
  }

  class RegionLogicProvider(onShardTerminatedCallback: () => Unit = () => {}) extends PerShardLogicProvider[String] {
    override def actorProvider(context: ActorContext): EntityPropsProvider[String] = (actorId: String) => props(actorId)

    override def healthCheck(): Future[HealthCheck] = Future {
      HealthCheck("producer-actor", "producer-actor-test", HealthCheckStatus.UP)
    }

    override def onShardTerminated(): Unit =
      onShardTerminatedCallback()

    override def start(): Unit = {}

    override def restart(): Unit = {}

    override def stop(): Unit = {}

    override def shutdown(): Unit = {}
  }
}

class TestActor(id: String) extends Actor {
  import TestActor._

  override def receive: Receive = onMessage(0)

  private def onMessage(value: Int): Receive = {
    case Update(_, newVal) => context.become(onMessage(newVal))
    case Delete(_)         => context.become(onMessage(0))
    case Get(_)            => sender() ! value
    case DoPassivate(_)    => context.parent ! Passivate(StopActor(sender()))
    case StopActor(originalSender) =>
      originalSender ! self // Send back a reference to this actor so we can manipulate it directly without having to go through the shard
    case Crash(_, ex) => throw ex
  }

}

class ShardSpec extends TestKit(ActorSystem("ShardSpec")) with AnyWordSpecLike with Matchers with MockitoSugar {
  import TestActor._
  private val shardProps = Shard.props("testShard", new RegionLogicProvider(), TestActor.idExtractor)(NoopTracerFactory.create())

  private def passivateActor(shard: ActorRef, childId: String): ActorRef = {
    val probe = TestProbe()
    probe.send(shard, DoPassivate(childId))
    probe.expectMsgType[ActorRef]
  }

  "Shard" should {
    "Properly route messages to children" in {
      val shard = system.actorOf(shardProps)
      val probe = TestProbe()
      val childId1 = "child1"
      val childId2 = "child2"

      probe.send(shard, Update(childId1, 1))
      probe.send(shard, Update(childId2, 2))

      probe.send(shard, Get(childId1))
      probe.expectMsg(1)

      probe.send(shard, Get(childId2))
      probe.expectMsg(2)

      probe.send(shard, Delete(childId1))
      probe.send(shard, Get(childId1))
      probe.expectMsg(0)

      probe.send(shard, Get(childId2))
      probe.expectMsg(2)
    }

    "Handle child id's with spaces" in {
      val shard = system.actorOf(shardProps)
      val probe = TestProbe()
      val childId1 = "child with spaces ~`!@#$%^&*()_-+=1"

      probe.send(shard, Update(childId1, 1))

      probe.send(shard, Get(childId1))
      probe.expectMsg(1)
    }

    "Send messages where the extracted entity id is empty to dead letters" in {
      val shard = system.actorOf(shardProps)
      val deadLetterProbe = TestProbe()
      system.eventStream.subscribe(deadLetterProbe.ref, classOf[DeadLetter])

      val emptyEntityId = Get("")
      shard ! emptyEntityId

      val dead = deadLetterProbe.expectMsgType[DeadLetter]
      dead.message shouldEqual emptyEntityId
      dead.sender shouldEqual shard
      dead.recipient shouldEqual system.deadLetters
    }

    "Allow child actors to stop completely if there are no pending messages for that child" in {
      val shard = system.actorOf(shardProps)
      val probe = TestProbe()
      val childId = "child1"

      val childActor = passivateActor(shard, childId)

      // Stop the actor, then send another message through the shard to recreate the actor.
      probe.watch(childActor)
      probe.send(childActor, PoisonPill)
      val terminated = probe.expectMsgType[Terminated]
      terminated.actor shouldEqual childActor
      probe.send(shard, Get(childId))
      probe.expectMsg(0)
    }

    "Ignore passivate messages from untracked entities" in {
      val shard = system.actorOf(shardProps)
      val probe = TestProbe()

      probe.send(shard, Passivate(""))
      probe.expectNoMessage()
    }

    "Buffer messages for child actors in the process of stopping" in {
      val shard = system.actorOf(shardProps)
      val probe = TestProbe()
      val childId = "child1"

      val childActor = passivateActor(shard, childId)

      // This update should be buffered by the shard
      probe.send(shard, Update(childId, 2))

      // Stop the actor, then send another message through the shard to recreate the actor.
      probe.watch(childActor)
      probe.send(childActor, PoisonPill)
      val terminated = probe.expectMsgType[Terminated]
      terminated.actor shouldEqual childActor
      probe.send(shard, Get(childId))
      probe.expectMsg(2)
    }

    "Not send a second stop message to a child that is already passivating" in {
      val shard = system.actorOf(shardProps)
      val probe = TestProbe()
      val childId = "child1"

      val childActor = passivateActor(shard, childId)

      probe.send(childActor, DoPassivate(childId))
      probe.expectNoMessage()
    }

    "Send messages to dead letters if the buffer for a child actor is already full" in {
      val shard = system.actorOf(shardProps)
      val probe = TestProbe()
      val childId = "child1"

      val deadLetterProbe = TestProbe()
      system.eventStream.subscribe(deadLetterProbe.ref, classOf[DeadLetter])

      passivateActor(shard, childId)
      // Fill buffer (default size 1000 messages)
      (1 to 1000).foreach(num => probe.send(shard, Update(childId, num)))

      val droppedMessage = Delete(childId)
      probe.send(shard, droppedMessage)

      val dead = deadLetterProbe.expectMsgType[DeadLetter]
      dead.message shouldEqual droppedMessage
      dead.sender shouldEqual shard
      dead.recipient shouldEqual system.deadLetters
    }

    "Executes onTerminate callback when shard actor dies" in {
      val probe = TestProbe()
      def notifyProbe(): Unit = {
        probe.ref ! Terminated
        ()
      }
      val shardActor = system.actorOf(Shard.props("testShard", new RegionLogicProvider(() => notifyProbe()), TestActor.idExtractor)(NoopTracerFactory.create()))
      probe.expectNoMessage()
      probe.send(shardActor, PoisonPill)
      probe.expectMsg(Terminated)
    }
  }
}
