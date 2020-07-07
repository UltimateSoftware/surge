// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams

import java.lang.Thread.UncaughtExceptionHandler

import com.ultimatesoftware.kafka.streams.KafkaStreamsUpdatePartitionsOnStateChangeListener.KafkaStateChange
import com.ultimatesoftware.kafka.streams.KafkaStreamsUncaughtExceptionHandler.KafkaStreamsUncaughtException
import com.ultimatesoftware.support.Logging
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.KafkaStreams.State
import org.apache.kafka.streams.processor.StateRestoreListener
import org.slf4j.LoggerFactory

trait KafkaStreamsStateChangeListenerCommon extends KafkaStreams.StateListener with Logging {

  val verbose: Boolean = true
  val streamName: String

  def onCreated(): Unit = {}
  def onRunning(): Unit = {}
  def onRebalancing(): Unit = {}
  def onError(): Unit = {}
  def onNotRunning(): Unit = {}
  def changed(change: KafkaStateChange): Unit = {}

  private def logMessage(msg: String): Unit = {
    if (verbose) log.info(msg)
  }
  override def onChange(newState: State, oldState: State): Unit = {
    newState match {
      case State.RUNNING if oldState == State.REBALANCING ⇒
        logMessage(s"Kafka stream $streamName state transitioned from REBALANCING to RUNNING")
        onRunning()
      case State.REBALANCING ⇒
        logMessage(s"Kafka stream $streamName state is REBALANCING")
        onRebalancing()
      case State.CREATED ⇒
        logMessage(s"Kafka stream $streamName state is CREATED")
        onCreated()
      case State.ERROR ⇒
        logMessage(s"Kafka stream $streamName shutting down because it transitioned to state ERROR")
        onError()
      case State.NOT_RUNNING ⇒
        logMessage(s"Kafka stream $streamName is NOT_RUNNING")
        onNotRunning()
      case _ ⇒
        log.debug("Kafka stream transitioning from {} to {}", Seq(oldState, newState): _*)
    }
    changed(KafkaStateChange(oldState, newState))
  }
}

class KafkaStreamsNotifyOnStateChangeListener(
    override val streamName: String,
    notifyTo: List[KafkaStateChange ⇒ Unit],
    override val verbose: Boolean = true) extends KafkaStreamsStateChangeListenerCommon {
  override def changed(change: KafkaStateChange): Unit = {
    if (notifyTo.nonEmpty) {
      notifyTo.foreach(f ⇒ f(change))
    }
  }
}
class KafkaStreamsUpdatePartitionsOnStateChangeListener(
    override val streamName: String,
    partitionTracker: KafkaStreamsPartitionTracker,
    override val verbose: Boolean = true) extends KafkaStreamsStateChangeListenerCommon {

  override def onRunning(): Unit =
    partitionTracker.update()

}

class KafkaStreamsStateChangeWithMultipleListeners(params: KafkaStreams.StateListener*) extends KafkaStreams.StateListener {
  override def onChange(newState: State, oldState: State): Unit =
    params.foreach(_.onChange(newState, oldState))
}

object KafkaStreamsUpdatePartitionsOnStateChangeListener {
  case class KafkaStateChange(oldState: State, newState: State)
}

class KafkaStreamsUncaughtExceptionHandler(notifyTo: List[KafkaStreamsUncaughtException ⇒ Unit] = List()) extends UncaughtExceptionHandler {
  private val log = LoggerFactory.getLogger(getClass)
  override def uncaughtException(t: Thread, e: Throwable): Unit = {
    log.warn(s"Kafka Streams uncaught exception is [$e]")
    log.error("Kafka Streams saw an uncaught exception", e)
    notifyTo.foreach(f ⇒ f(KafkaStreamsUncaughtException(t, e)))
  }
}
object KafkaStreamsUncaughtExceptionHandler {
  case class KafkaStreamsUncaughtException(thread: Thread, exception: Throwable)
}

class KafkaStreamsStateRestoreListener extends StateRestoreListener with Logging {
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
