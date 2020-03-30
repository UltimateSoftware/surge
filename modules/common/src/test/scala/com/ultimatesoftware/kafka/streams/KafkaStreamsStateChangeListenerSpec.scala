// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams

import org.apache.kafka.common.TopicPartition
import org.apache.kafka.streams.KafkaStreams.State
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.mockito.MockitoSugar

class KafkaStreamsStateChangeListenerSpec extends AnyWordSpec with Matchers with MockitoSugar {
  trait TestContext {
    val tracker: KafkaStreamsPartitionTracker = mock[KafkaStreamsPartitionTracker]
    doNothing().when(tracker).update()
    val exiter: Exiter = mock[Exiter]
    doNothing().when(exiter).exit(anyInt)

    val stateChangeListener = new KafkaStreamsStateChangeListener(tracker, exiter)
    val exceptionHandler = new KafkaStreamsUncaughtExceptionHandler(exiter)
    val stateRestoreListener = new KafkaStreamsStateRestoreListener
  }
  "KafkaStreamsStateChangeListener" should {
    "Update the partition tracker when transitioning from rebalancing to running" in new TestContext {
      stateChangeListener.onChange(newState = State.RUNNING, oldState = State.REBALANCING)
      verify(tracker).update()
    }

    "Exit if the stream process goes to an error state and then into a not running state" in new TestContext {
      stateChangeListener.onChange(newState = State.ERROR, oldState = State.RUNNING)
      stateChangeListener.onChange(newState = State.NOT_RUNNING, oldState = State.ERROR)
      verify(exiter).exit(1)
    }
  }

  "KafkaStreamsUncaughtExceptionHandler" should {
    "Exit if the stream thread hits an uncaught exception" in new TestContext {
      exceptionHandler.uncaughtException(Thread.currentThread(), new RuntimeException("This is expected"))
      verify(exiter).exit(1)
    }
  }

  "KafkaStreamsStateRestoreListener" should {
    "Follow updates to state restores" in new TestContext {
      val exampleTopicPartition = new TopicPartition("test", 0)
      val stateStoreName = "exampleStateStore"
      stateRestoreListener.onRestoreStart(exampleTopicPartition, stateStoreName, 0L, 10L)
      stateRestoreListener.onBatchRestored(exampleTopicPartition, stateStoreName, 10L, 10L)
      stateRestoreListener.onRestoreEnd(exampleTopicPartition, stateStoreName, 10L)
    }
  }
}
