// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams

import java.lang.Thread.UncaughtExceptionHandler

import org.apache.kafka.common.TopicPartition
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.processor.StateRestoreListener
import org.slf4j.{ Logger, LoggerFactory }

class KafkaStreamsStateChangeListener(partitionTracker: KafkaStreamsPartitionTracker) extends KafkaStreams.StateListener {
  private val log: Logger = LoggerFactory.getLogger(getClass)

  // FIXME replace this with a real supervisor
  private var hitError: Boolean = false
  override def onChange(newState: KafkaStreams.State, oldState: KafkaStreams.State): Unit = {
    log.debug(s"Kafka streams transitioning from $oldState to $newState")

    if ((oldState == KafkaStreams.State.REBALANCING) && (newState == KafkaStreams.State.RUNNING)) {
      log.info("Kafka streams transitioned from rebalancing to running")
      partitionTracker.update()
    }

    if (newState == KafkaStreams.State.ERROR) {
      hitError = true
    }

    if (hitError && newState == KafkaStreams.State.NOT_RUNNING) {
      log.error("Kafka streams shutting down because of an error")
      sys.exit(1)
    }
  }
}

// FIXME we may be able to do a clean close/restart of the stream itself
class KafkaStreamsUncaughtExceptionHandler extends UncaughtExceptionHandler {
  private val log: Logger = LoggerFactory.getLogger(getClass)
  override def uncaughtException(t: Thread, e: Throwable): Unit = {
    log.error(s"Unhandled exception in thread $t", e)
    log.error("Kafka streams shutting down because of an error")
    sys.exit(1)
  }
}

class KafkaStreamsStateRestoreListener extends StateRestoreListener {
  private val log: Logger = LoggerFactory.getLogger(getClass)
  override def onRestoreStart(topicPartition: TopicPartition, storeName: String, startingOffset: Long, endingOffset: Long): Unit = {
    log.info(s"Restore started for $storeName on topicPartition " +
      s"${topicPartition.topic()}:${topicPartition.partition()}, offsets $startingOffset -> $endingOffset")
  }

  override def onBatchRestored(topicPartition: TopicPartition, storeName: String, batchEndOffset: Long, numRestored: Long): Unit = {
    log.trace(s"Batch restored for $storeName on topicPartition " +
      s"${topicPartition.topic()}:${topicPartition.partition()}, end offset = $batchEndOffset, number restored = $numRestored")
  }

  override def onRestoreEnd(topicPartition: TopicPartition, storeName: String, totalRestored: Long): Unit = {
    log.info(s"Restore finished for $storeName on topicPartition " +
      s"${topicPartition.topic()}:${topicPartition.partition()}, total restored = $totalRestored")
  }
}
