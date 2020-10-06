// Copyright © 2018-2020 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams

import akka.actor.ActorRef
import akka.pattern._
import akka.util.Timeout
import com.ultimatesoftware.config.TimeoutConfig
import com.ultimatesoftware.kafka.KafkaConsumerStateTrackingActor
import org.apache.kafka.streams.KafkaStreams
import org.slf4j.LoggerFactory

class KafkaStreamsPartitionTrackerActorProvider(managementActor: ActorRef) extends KafkaStreamsPartitionTrackerProvider {
  override def create(streams: KafkaStreams): KafkaStreamsPartitionTracker = new KafkaStreamsPartitionTrackerActorImpl(streams, managementActor)
}

class KafkaStreamsPartitionTrackerActorImpl(kafkaStreams: KafkaStreams, currentManagementActor: ActorRef) extends KafkaStreamsPartitionTracker(kafkaStreams) {
  private val log = LoggerFactory.getLogger(getClass)
  def update(): Unit = {
    val metaByInstance = metadataByInstance()

    implicit val timeout: Timeout = Timeout(TimeoutConfig.PartitionTracker.updateTimeout)
    (currentManagementActor ? KafkaConsumerStateTrackingActor.StateUpdated(metaByInstance)).map { _ ⇒
      log.debug(s"Cluster state successfully updated to {}", metaByInstance)
    }(scala.concurrent.ExecutionContext.global)
  }
}
