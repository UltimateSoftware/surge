// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.streams.replay

import akka.Done
import akka.actor.{Actor, ActorRef, ActorSystem, Props, Stash}
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRebalanceListener, KafkaConsumer, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition
import surge.internal.streams.PartitionAssignorConfig
import surge.internal.utils.Logging
import surge.kafka.{KafkaBytesConsumer, UltiKafkaConsumerConfig}
import surge.streams.replay.TopicResetActor._

import java.util
import java.util.concurrent.TimeUnit
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._

class KafkaForeverReplayStrategy(
    actorSystem: ActorSystem,
    settings: KafkaForeverReplaySettings,
    override val preReplay: () => Future[Any] = { () => Future.successful(true) },
    override val postReplay: () => Unit = { () => () },
    override val replayProgress: Double => Unit = { _ => ()})(implicit executionContext: ExecutionContext)
    extends EventReplayStrategy {

  override def createReplayController[Key, Value](context: ReplayControlContext[Key, Value]): KafkaForeverReplayControl =
    new KafkaForeverReplayControl(actorSystem, settings, preReplay, postReplay)
}

class KafkaForeverReplayControl(
    actorSystem: ActorSystem,
    settings: KafkaForeverReplaySettings,
    override val preReplay: () => Future[Any] = { () => Future.successful(true) },
    override val postReplay: () => Unit = { () => () },
    override val replayProgress: Double => Unit = { _ => ()})(implicit executionContext: ExecutionContext)
    extends ReplayControl {
  private val underlyingActor = actorSystem.actorOf(Props(new TopicResetActor(settings.brokers, settings.topic)))

  implicit val timeout: Timeout = Timeout(settings.resetTopicTimeout)
  override def fullReplay(consumerGroup: String, partitions: Iterable[Int]): Future[Done] = {
    underlyingActor.ask(TopicResetActor.ResetTopic(consumerGroup, partitions.toList)).mapTo[ResetTopicResult].map {
      case TopicResetSucceed     => Done
      case TopicResetFailed(err) => throw err
    }
  }

  override def getReplayProgress(): Future[Double] = ???
}

trait EventReplaySettings {
  val entireReplayTimeout: FiniteDuration
}

case class KafkaForeverReplaySettings(brokers: List[String], resetTopicTimeout: FiniteDuration, entireReplayTimeout: FiniteDuration, topic: String)
    extends EventReplaySettings

object KafkaForeverReplaySettings {
  private val config = ConfigFactory.load()
  private val brokers = config.getString("kafka.brokers").split(",").toList
  private val resetTopicTimeout: FiniteDuration = config.getDuration("kafka.streams.replay.reset-topic-timeout", TimeUnit.MILLISECONDS).milliseconds
  private val entireProcessTimeout: FiniteDuration = config.getDuration("kafka.streams.replay.entire-process-timeout", TimeUnit.MILLISECONDS).milliseconds

  def apply(topic: String): KafkaForeverReplaySettings = {
    new KafkaForeverReplaySettings(brokers, resetTopicTimeout, entireProcessTimeout, topic)
  }

  def create(topic: String): KafkaForeverReplaySettings = apply(topic)
}

object KafkaForeverReplayStrategy {
  def create[Key, Value](
      actorSystem: ActorSystem,
      settings: KafkaForeverReplaySettings,
      preReplay: () => Future[Any] = { () => Future.successful(true) },
      postReplay: () => Unit = { () => () },
      replayProgress: Double => Unit = { _ => ()}): KafkaForeverReplayStrategy = {
    new KafkaForeverReplayStrategy(actorSystem, settings, preReplay, postReplay, replayProgress)(ExecutionContext.global)
  }
}

class TopicResetActor(brokers: List[String], kafkaTopic: String) extends Actor with Stash with Logging {

//  TODO Create consumer and admin clients here

  def ready: Receive = {
    case ResetTopic(consumerGroup, partitions) =>
      try {
        initialize(consumerGroup, partitions)
      } catch {
        case err: Throwable =>
          handleError(err, sender())
      }
  }

  def initializing(replyTo: ActorRef, consumer: KafkaConsumer[String, Array[Byte]], kafkaTopic: String, topicPartitions: List[TopicPartition]): Receive = {
    case PartitionsAssigned =>
      try {
        val preReplayCommits = consumer.committed(topicPartitions.toSet.asJava).asScala.map( kv => kv._1.partition() -> kv._2)
        val earliestCommits = buildEarliestCommits(consumer, topicPartitions).asJava
        consumer.commitSync(earliestCommits)
        replyTo ! TopicResetSucceed
        context.become(waitingForComplete(replyTo, consumer, kafkaTopic, topicPartitions, Map.from(preReplayCommits)))
        unstashAll()
      } catch {
        case err: Throwable =>
          handleError(err, replyTo)
      } finally {
        consumer.close()
      }
      log.debug(s"Replay Started for $kafkaTopic") //TODO Report starting offsets
    case _: ResetTopic =>
      stash()
  }

  def waitingForComplete(replyTo: ActorRef, consumer: KafkaConsumer[String, Array[Byte]], kafkaTopic: String,
                         topicPartitions: List[TopicPartition], preReplayCommits: Map[Int, OffsetAndMetadata]): Receive = {
    case CheckProgress =>
      try {
        val currentCommits = consumer.committed(topicPartitions.toSet.asJava).asScala //TODO Use Admin API to
        val complete = topicPartitions.foldLeft(true){ case (b, p) =>
          b & currentCommits(p).offset() >= preReplayCommits(p.partition()).offset()
        }
        if(complete) {
          replyTo ! TopicReplayComplete
          context.become(ready)
        } else {
          log.debug(s"Replay in Progress for $kafkaTopic (${currentCommits})")
          // TODO Report this via a TopicReplayProgress message?
          // TODO configure this poll time
          context.system.scheduler.scheduleOnce(30.seconds, self, CheckProgress)(context.dispatcher)
        }
      } catch {
        case err: Throwable =>
          handleError(err, replyTo)
      } finally {
        consumer.close()
      }
  }

  def handleError(err: Throwable, replyTo: ActorRef): Unit = {
    log.error(s"Topic reset failed for $kafkaTopic", err)
    replyTo ! TopicResetFailed(err)
    context.become(ready)
    unstashAll()
  }

  def initialize(consumerGroup: String, partitions: List[Int]): Unit = {
    log.debug(s"Replay started for $kafkaTopic")
    val topicPartitions = partitions.map { partition => new TopicPartition(kafkaTopic, partition) }
    // TODO Create admin client here
    val consumer = KafkaBytesConsumer(
      brokers,
      UltiKafkaConsumerConfig(consumerGroup),
      Map(
        ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG -> "60000",
        ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG -> PartitionAssignorConfig.assignorClassName)).consumer
    consumer.subscribe(
      List(kafkaTopic).asJava,
      new ConsumerRebalanceListener() {
        override def onPartitionsAssigned(partitions: util.Collection[TopicPartition]): Unit = {
          if (partitions.size != topicPartitions.size) {
            throw new IllegalStateException(
              s"There are (${topicPartitions.size - partitions.size})" +
                s" active consumers for topic $kafkaTopic we can't proceed with replaying")
          }
          self ! PartitionsAssigned
        }
        override def onPartitionsRevoked(partitions: util.Collection[TopicPartition]): Unit = {}
      })
    // NOTE: non-deprecated #poll(Duration) method doesn't work because it includesMetadataInTimeout opposite to this one
    consumer.poll(1000)
    context.become(initializing(sender(), consumer, kafkaTopic, topicPartitions))
    // TODO configure this poll time
    context.system.scheduler.scheduleOnce(30.seconds, self, CheckProgress)(context.dispatcher)
  }

  def buildEarliestCommits(consumer: KafkaConsumer[String, Array[Byte]], topicPartitions: List[TopicPartition]): Map[TopicPartition, OffsetAndMetadata] = {
    val beginningOffsets = consumer.beginningOffsets(topicPartitions.asJava)
    topicPartitions.map { tp =>
      val firstOffset = beginningOffsets.getOrDefault(tp, 0L)
      val offset = new OffsetAndMetadata(firstOffset)
      tp -> offset
    }.toMap
  }

  override def receive: Receive = ready
}

private[streams] object TopicResetActor {
  sealed trait ResetTopicCommand
  case class ResetTopic(consumerGroup: String, partitions: List[Int]) extends ResetTopicCommand
  case object CheckProgress

  sealed trait ResetTopicEvent
  case object PartitionsAssigned extends ResetTopicEvent

  sealed trait ResetTopicResult
  case object TopicResetSucceed extends ResetTopicResult
  case class TopicReplayProgress(percentComplete: Double) extends ResetTopicResult
  case object TopicReplayComplete extends ResetTopicResult
  case class TopicResetFailed(err: Throwable) extends ResetTopicResult

}
