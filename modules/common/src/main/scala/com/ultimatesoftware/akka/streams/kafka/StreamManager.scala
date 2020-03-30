// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.akka.streams.kafka

import java.util.concurrent.atomic.AtomicReference

import akka.Done
import akka.actor.{ Actor, ActorSystem, Props }
import akka.kafka.scaladsl.Consumer.DrainingControl
import akka.kafka.scaladsl.{ Committer, Consumer }
import akka.kafka.{ CommitterSettings, ConsumerSettings, Subscriptions }
import akka.pattern._
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{ RestartSource, Sink }
import com.ultimatesoftware.akka.cluster.{ ActorHostAwareness, ActorSystemHostAwareness }
import com.ultimatesoftware.akka.streams.kafka.KafkaStreamManagerActor.StartConsuming
import com.ultimatesoftware.scala.core.kafka.KafkaTopic
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.TopicPartition

import scala.concurrent.Future
import scala.concurrent.duration._

class KafkaStreamManager(topic: KafkaTopic, groupId: String,
    business: (String, Array[Byte]) ⇒ Future[Done],
    parallelism: Int = 1)(implicit val actorSystem: ActorSystem) extends ActorSystemHostAwareness {

  private val managerActor = actorSystem.actorOf(Props(new KafkaStreamManagerActor(topic, groupId, business, parallelism)))

  def start(): KafkaStreamManager = {
    managerActor ! StartConsuming
    this
  }

  private def committableSourceWithOffsets(
    offsets: Map[TopicPartition, Long],
    consumerSettings: ConsumerSettings[String, Array[Byte]]): Unit = {

    val newConsumerSettings = consumerSettings
      .withProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, classOf[HostAwareRangeAssignor].getName)
      .withProperty(HostAwarenessConfig.HOST_CONFIG, localHostname)
      .withProperty(HostAwarenessConfig.PORT_CONFIG, localPort.toString)

    Consumer.committableSource(newConsumerSettings, Subscriptions.assignmentWithOffset(offsets))
  }
}

object KafkaStreamManagerActor {
  case object StartConsuming
  case object StopConsuming
  case object SuccessfullyStopped
}
class KafkaStreamManagerActor(topic: KafkaTopic, groupId: String,
    business: (String, Array[Byte]) ⇒ Future[Done],
    parallelism: Int) extends Actor with ActorHostAwareness with KafkaConsumerTrait {
  import KafkaStreamManagerActor._
  import context.dispatcher
  private implicit val materializer: ActorMaterializer = ActorMaterializer()

  private lazy val consumerSettings = KafkaConsumer.consumerSettings(context.system, groupId)
    .withProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, classOf[HostAwareRangeAssignor].getName)
    .withProperty(HostAwarenessConfig.HOST_CONFIG, localHostname)
    .withProperty(HostAwarenessConfig.PORT_CONFIG, localPort.toString)

  override def receive: Receive = stopped

  private def stopped: Receive = {
    case StartConsuming ⇒ startConsumer()
  }

  private def consuming(control: DrainingControl[Done]): Receive = {
    case StopConsuming       ⇒ handleStopConsuming(control)
    case SuccessfullyStopped ⇒ handleSuccessfullyStopped()
  }

  private def startConsumer(): Unit = {
    val committerSettings = CommitterSettings(context.system)
    val control = new AtomicReference[Consumer.Control](Consumer.NoopControl)

    val result = RestartSource
      .onFailuresWithBackoff(
        minBackoff = 3.seconds,
        maxBackoff = 30.seconds,
        randomFactor = 0.2) { () ⇒
        createCommittableSource(topic, consumerSettings)
          .mapAsync(parallelism)(msg ⇒ business(msg.record.key, msg.record.value).map(_ ⇒ msg.committableOffset))
          .via(Committer.flow(committerSettings))
          .mapMaterializedValue(c ⇒ control.set(c))
      }.runWith(Sink.ignore)

    val drainingControl = DrainingControl(control.get() -> result)

    context.become(consuming(drainingControl))
  }

  private def handleStopConsuming(control: DrainingControl[Done]): Unit = {
    control.drainAndShutdown().map(_ ⇒ SuccessfullyStopped) pipeTo self
  }

  private def handleSuccessfullyStopped(): Unit = {
    context.become(stopped)
  }
}
