// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.persistence

import akka.actor.{ ActorContext, ActorSystem, Props }
import com.typesafe.config.Config
import org.apache.kafka.common.TopicPartition
import play.api.libs.json.JsValue
import surge.akka.cluster.{ EntityPropsProvider, PerShardLogicProvider }
import surge.core.{ Ack, Controllable, KafkaProducerActor }
import surge.health.HealthSignalBusTrait
import surge.internal.akka.kafka.KafkaConsumerPartitionAssignmentTracker
import surge.internal.health.HealthCheck
import surge.internal.persistence
import surge.internal.utils.Logging
import surge.kafka.streams.AggregateStateStoreKafkaStreams
import surge.kafka.{ PersistentActorRegionCreator => KafkaPersistentActorRegionCreator }
import surge.metrics.Metrics

import scala.concurrent.{ ExecutionContext, Future }
import scala.util.{ Failure, Success, Try }

trait PersistentActorPropsFactory[M] extends {
  def props(aggregateId: String, businessLogic: BusinessLogic, resources: PersistentEntitySharedResources): Props
}

class PersistentActorRegionCreator[M](
    system: ActorSystem,
    businessLogic: BusinessLogic,
    kafkaStreamsCommand: AggregateStateStoreKafkaStreams[JsValue],
    partitionTracker: KafkaConsumerPartitionAssignmentTracker,
    metrics: Metrics,
    signalBus: HealthSignalBusTrait,
    config: Config)
    extends KafkaPersistentActorRegionCreator[String] {
  override def regionFromTopicPartition(topicPartition: TopicPartition): PerShardLogicProvider[String] =
    new PersistentActorRegion[M](system, topicPartition, businessLogic, kafkaStreamsCommand, partitionTracker, metrics, signalBus, config)
}

class PersistentActorRegion[M](
    system: ActorSystem,
    assignedPartition: TopicPartition,
    businessLogic: BusinessLogic,
    aggregateKafkaStreamsImpl: AggregateStateStoreKafkaStreams[JsValue],
    partitionTracker: KafkaConsumerPartitionAssignmentTracker,
    metrics: Metrics,
    signalBus: HealthSignalBusTrait,
    val config: Config)
    extends PerShardLogicProvider[String]
    with Logging {

  private val kafkaProducerActor: KafkaProducerActor = KafkaProducerActor(
    actorSystem = system,
    assignedPartition = assignedPartition,
    metrics = metrics,
    businessLogic = businessLogic,
    partitionTracker = partitionTracker,
    kStreams = aggregateKafkaStreamsImpl,
    signalBus = signalBus,
    config = config)

  override def onShardTerminated(): Unit = {
    log.debug("Shard for partition {} terminated, killing partition kafkaProducerActor", assignedPartition)
    kafkaProducerActor.terminate()
  }

  override def healthCheck(): Future[HealthCheck] = {
    kafkaProducerActor.healthCheck()
  }

  override def actorProvider(context: ActorContext): EntityPropsProvider[String] = {
    val aggregateMetrics = PersistentActor.createMetrics(metrics, businessLogic.aggregateName)
    //FIXME: temporary fix to support switch between akka and existing shard allocation strategy
    val aggregateIdToKafkaProducer = (_: String) => kafkaProducerActor
    val sharedResources = persistence.PersistentEntitySharedResources(aggregateIdToKafkaProducer, aggregateMetrics, aggregateKafkaStreamsImpl)

    actorId: String => PersistentActor.props(businessLogic, sharedResources, config, Some(actorId))
  }

  private def registrationCallback(): PartialFunction[Try[Ack], Unit] = {
    case Success(_) =>
      val registrationResult =
        signalBus.register(
          control = this.controllable,
          componentName = s"persistent-actor-region-${assignedPartition.topic()}-${assignedPartition.partition()}",
          restartSignalPatterns = restartSignalPatterns())

      registrationResult.onComplete {
        case Failure(exception) =>
          log.error(s"${this.getClass} registration failed", exception)
        case Success(_) =>
          log.debug(s"${this.getClass} registration succeeded")
      }(ExecutionContext.global)
    case Failure(error) =>
      log.error(s"Unable to register ${this.getClass} for supervision", error)
  }

  private[surge] override val controllable: Controllable = new Controllable {
    override def start(): Future[Ack] = kafkaProducerActor.controllable.start().andThen(registrationCallback())(ExecutionContext.global)

    override def restart(): Future[Ack] = {
      implicit val executionContext: ExecutionContext = system.dispatcher
      for {
        _ <- stop()
        started <- start()
      } yield {
        started
      }
    }

    override def stop(): Future[Ack] = {
      kafkaProducerActor.controllable.stop()
    }

    override def shutdown(): Future[Ack] = stop()
  }
}
