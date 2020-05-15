// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.akka.streams.kafka

import java.util.UUID
import java.util.concurrent.atomic.AtomicReference

import akka.Done
import akka.actor.{ Actor, ActorSystem, Props, Stash }
import akka.kafka.scaladsl.Consumer.DrainingControl
import akka.kafka.scaladsl.{ Committer, Consumer }
import akka.kafka.{ CommitterSettings, ConsumerMessage, ConsumerSettings, Subscriptions }
import akka.pattern._
import akka.stream.scaladsl.{ Flow, RestartSource, Sink }
import com.ultimatesoftware.akka.cluster.{ ActorHostAwareness, ActorSystemHostAwareness }
import com.ultimatesoftware.scala.core.kafka.KafkaTopic
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.slf4j.LoggerFactory

import scala.concurrent.Future
import scala.concurrent.duration._

class KafkaStreamManager[Key, Value](topic: KafkaTopic, consumerSettings: ConsumerSettings[Key, Value],
    business: (Key, Value) ⇒ Future[Any],
    parallelism: Int = 1)(implicit val actorSystem: ActorSystem) extends ActorSystemHostAwareness {

  private val managerActor = actorSystem.actorOf(Props(new KafkaStreamManagerActor(topic, consumerSettings, business, parallelism)))

  def start(): KafkaStreamManager[Key, Value] = {
    managerActor ! KafkaStreamManagerActor.StartConsuming
    this
  }

  def stop(): KafkaStreamManager[Key, Value] = {
    managerActor ! KafkaStreamManagerActor.StopConsuming
    this
  }
}

object KafkaStreamManagerActor {
  case object StartConsuming
  case object StopConsuming
  case object SuccessfullyStopped
}
class KafkaStreamManagerActor[Key, Value](topic: KafkaTopic, baseConsumerSettings: ConsumerSettings[Key, Value],
    business: (Key, Value) ⇒ Future[Any],
    parallelism: Int) extends Actor with ActorHostAwareness with Stash {
  import KafkaStreamManagerActor._
  import context.{ dispatcher, system }
  private val log = LoggerFactory.getLogger(getClass)

  // Set this uniquely per manager actor so that restarts of the Kafka stream don't cause a rebalance of the consumer group
  private val clientId = s"surge-event-source-managed-consumer-${UUID.randomUUID()}"

  private lazy val businessFlow = Flow[ConsumerMessage.CommittableMessage[Key, Value]].mapAsync(parallelism) { msg ⇒
    business(msg.record.key, msg.record.value)
      .map(_ ⇒ msg.committableOffset)
      .recover {
        case e ⇒
          log.error(s"An exception was thrown by the event handler in consumer $clientId! The stream will restart and the message will be retried.", e)
          throw e
      }
  }

  private lazy val consumerSettings = baseConsumerSettings
    .withProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, classOf[HostAwareRangeAssignor].getName)
    .withProperty(HostAwarenessConfig.HOST_CONFIG, localHostname)
    .withProperty(HostAwarenessConfig.PORT_CONFIG, localPort.toString)
    .withStopTimeout(Duration.Zero)
  // TODO enable these for smoother stream restarts once the CMP Kafka brokers are updated to version 2.3.0+,
  //  also update the unit test to verify smoother restarts
  //.withClientId(clientId)
  //.withGroupInstanceId(clientId)

  private case class InternalState(control: AtomicReference[Consumer.Control], streamCompletion: Future[Done])

  override def receive: Receive = stopped

  private def stopped: Receive = {
    case StartConsuming ⇒ startConsumer()
  }

  private def consuming(state: InternalState): Receive = {
    case StopConsuming ⇒ handleStopConsuming(state)
  }

  private def stopping(state: InternalState): Receive = {
    case SuccessfullyStopped ⇒ handleSuccessfullyStopped()
    case _                   ⇒ stash()
  }

  private def startConsumer(): Unit = {
    log.info("Starting consumer for topic {} with client id {}", Seq(topic.name, clientId): _*)
    val committerSettings = CommitterSettings(context.system)
    val control = new AtomicReference[Consumer.Control](Consumer.NoopControl)

    val result = RestartSource
      .onFailuresWithBackoff(
        minBackoff = 1.second,
        maxBackoff = 15.seconds,
        randomFactor = 0.1) { () ⇒
        log.debug("Creating Kafka source for topic {} with client id {}", Seq(topic.name, clientId): _*)
        Consumer
          .committableSource(consumerSettings, Subscriptions.topics(topic.name))
          .mapMaterializedValue(c ⇒ control.set(c))
          .via(businessFlow)
          .via(Committer.flow(committerSettings))
      }.runWith(Sink.ignore)

    val state = InternalState(control, result)

    context.become(consuming(state))
  }

  private def handleStopConsuming(state: InternalState): Unit = {
    val control = state.control.get()
    val drainingControl = DrainingControl(control -> state.streamCompletion)
    log.info("Stopping consumer with client id {} for topic {}", Seq(clientId, topic.name): _*)
    val shutdownFuture = drainingControl.drainAndShutdown().map(_ ⇒ SuccessfullyStopped)

    shutdownFuture.pipeTo(self)(sender())
    context.become(stopping(state))
  }

  private def handleSuccessfullyStopped(): Unit = {
    log.info("Consumer with client id {} for topic {} successfully stopped", Seq(clientId, topic.name): _*)
    context.become(stopped)
    unstashAll()
  }
}
