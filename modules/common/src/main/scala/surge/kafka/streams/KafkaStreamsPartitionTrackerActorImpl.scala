// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.kafka.streams

import akka.actor.ActorRef
import akka.pattern._
import akka.util.Timeout
import org.apache.kafka.streams.KafkaStreams
import org.slf4j.LoggerFactory
import surge.config.TimeoutConfig
import surge.kafka.KafkaConsumerStateTrackingActor

class KafkaStreamsPartitionTrackerActorProvider(managementActor: ActorRef) extends KafkaStreamsPartitionTrackerProvider {
  override def create(streams: KafkaStreams): KafkaStreamsPartitionTracker = new KafkaStreamsPartitionTrackerActorImpl(streams, managementActor)
}

class KafkaStreamsPartitionTrackerActorImpl(kafkaStreams: KafkaStreams, currentManagementActor: ActorRef) extends KafkaStreamsPartitionTracker(kafkaStreams) {
  private val log = LoggerFactory.getLogger(getClass)
  def update(): Unit = {
    val metaByInstance = metadataByInstance()

    implicit val timeout: Timeout = Timeout(TimeoutConfig.PartitionTracker.updateTimeout)
    (currentManagementActor ? KafkaConsumerStateTrackingActor.StateUpdated(metaByInstance)).map { _ =>
      log.debug(s"Cluster state successfully updated to {}", metaByInstance)
    }(scala.concurrent.ExecutionContext.global)
  }
}
