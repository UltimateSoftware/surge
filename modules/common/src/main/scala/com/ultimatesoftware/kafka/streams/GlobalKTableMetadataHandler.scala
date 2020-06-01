// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams

import akka.actor.{ ActorRef, ActorSystem, Props }
import akka.util.Timeout
import com.ultimatesoftware.config.{ BackoffConfig, TimeoutConfig }
import com.ultimatesoftware.kafka.streams.GlobalKTableMetadataHandlerImpl._
import com.ultimatesoftware.scala.core.kafka.{ KafkaStringProducer, KafkaTopic }
import org.apache.kafka.streams.scala.kstream.KStream
import akka.pattern.{ BackoffOpts, BackoffSupervisor, ask }
import com.ultimatesoftware.support.{ BackoffChildActorTerminationWatcher, Logging, SystemExit }
import org.apache.kafka.streams.Topology
import play.api.libs.json.Json

import scala.concurrent.{ ExecutionContext, Future }

object GlobalStreamsWriteBufferSettings extends WriteBufferSettings {
  override def maxWriteBufferNumber: Int = 2
  override def writeBufferSizeMb: Int = 4
}

object GlobalStreamsBlockCacheSettings extends BlockCacheSettings {
  override def blockSizeKb: Int = 16
  override def blockCacheSizeMb: Int = 1
  override def cacheIndexAndFilterBlocks: Boolean = false
}

class KafkaPartitionMetadataGlobalStreamsRocksDBConfig extends CustomRocksDBConfigSetter(GlobalStreamsBlockCacheSettings, GlobalStreamsWriteBufferSettings)

class GlobalKTableMetadataHandler(
    internalMetadataTopic: KafkaTopic,
    consumerGroupName: String,
    system: ActorSystem) extends KafkaPartitionMetadataHandler with HealthyComponent with Logging {

  private[streams] lazy val settings = GlobalKTableMetadataHandlerImplSettings(consumerGroupName, internalMetadataTopic.name)

  private[streams] val underlyingActor = createUnderlyingActorWithBackOff()

  private implicit val askTimeoutDuration: Timeout = TimeoutConfig.GlobalKTableKafkaStreamActor.askTimeout

  override def processPartitionMetadata(stream: KStream[String, KafkaPartitionMetadata]): Unit = {
    val producer = KafkaStringProducer(settings.brokers, internalMetadataTopic)
    stream.mapValues { value ⇒
      log.trace("Updating StateMeta for {} to {}", Seq(value.topicPartition, value): _*)
      producer.putKeyValue(value.topicPartition, Json.toJson(value).toString())
    }
  }

  override def healthCheck(): Future[HealthCheck] =
    underlyingActor.ask(HealthyActor.GetHealth).mapTo[HealthCheck]

  def isOpen()(implicit executionContext: ExecutionContext): Future[Boolean] = {
    underlyingActor.ask(IsOpen).mapTo[Boolean].recoverWith {
      case err: Throwable ⇒
        log.error("Failed to determine whether GlobalKTable stream is open or not", err)
        Future.successful(false)
    }
  }

  def getMeta(topicPartitionKey: String): Future[Option[KafkaPartitionMetadata]] = {
    underlyingActor.ask(GetMeta(topicPartitionKey)).mapTo[Option[KafkaPartitionMetadata]]
  }

  private[streams] def createBackoffSupervisorFor(childProps: Props): Props =
    BackoffSupervisor.props(
      BackoffOpts.onStop(
        childProps,
        childName = settings.storeName,
        minBackoff = BackoffConfig.GlobalKTableKafkaStreamActor.minBackoff,
        maxBackoff = BackoffConfig.GlobalKTableKafkaStreamActor.maxBackoff,
        randomFactor = BackoffConfig.GlobalKTableKafkaStreamActor.randomFactor)
        .withMaxNrOfRetries(BackoffConfig.GlobalKTableKafkaStreamActor.maxRetries))

  private[streams] def createUnderlyingActorWithBackOff(): ActorRef = {
    def onMaxRetries(): Unit = {
      log.error(s"Kafka stream ${settings.storeName} failed more than the max number of retries, Surge is killing the JVM")
      SystemExit.exit(1)
    }

    val globalKTableMetadataHandlerImplProps = GlobalKTableMetadataHandlerImpl.props(
      internalMetadataTopic,
      settings)

    val underlyingActorProps = createBackoffSupervisorFor(globalKTableMetadataHandlerImplProps)

    val underlyingCreatedActor = system.actorOf(underlyingActorProps)

    system.actorOf(BackoffChildActorTerminationWatcher.props(underlyingCreatedActor, onMaxRetries))

    underlyingCreatedActor
  }

  private[streams] def getTopology()(implicit executionContext: ExecutionContext): Future[Topology] = {
    underlyingActor.ask(GetTopology).mapTo[Topology]
  }
}
