// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.kafka.streams

import org.apache.kafka.common.TopicPartition
import org.apache.kafka.streams.KafkaStreams.State
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.mockito.MockitoSugar
import surge.kafka.streams.KafkaStreamsUncaughtExceptionHandler.KafkaStreamsUncaughtException
import surge.kafka.streams.KafkaStreamsUpdatePartitionsOnStateChangeListener.KafkaStateChange

class KafkaStreamsUpdatePartitionsOnStateChangeListenerSpec extends AnyWordSpec with Matchers with MockitoSugar {
  trait TestContext {
    val tracker: KafkaStreamsPartitionTracker = mock[KafkaStreamsPartitionTracker]
    doNothing().when(tracker).update()

    val stateRestoreListener = new KafkaStreamsStateRestoreListener
  }
  class ExceptionListenerInside {
    def listenException(exception: KafkaStreamsUncaughtException): Unit = {
      // Do nothing
    }
  }
  class StateChangeListenerInside {
    def listenStateChange(change: KafkaStateChange): Unit = {
      // Do nothing
    }
  }
  "KafkaStreamsUpdatePartitionsOnStateChangeListener" should {
    "Update the partition tracker when transitioning from rebalancing to running" in new TestContext {
      val stateChangeListener = new KafkaStreamsUpdatePartitionsOnStateChangeListener("TestStreamName", tracker)
      stateChangeListener.onChange(newState = State.RUNNING, oldState = State.REBALANCING)
      verify(tracker).update()
    }
  }

  "KafkaStreamsNotifyOnStateChangeListener" should {
    "Notify when changing states" in new TestContext {
      val notifyTo = mock[StateChangeListenerInside]
      val stateChangeListener = new KafkaStreamsNotifyOnStateChangeListener("TestStreamName", List(notifyTo.listenStateChange))
      stateChangeListener.onChange(newState = State.RUNNING, oldState = State.REBALANCING)
      verify(notifyTo).listenStateChange(any[KafkaStateChange])
    }
  }

  "KafkaStreamsUncaughtExceptionHandler" should {
    "Notifies about any uncaught exception" in new TestContext {
      val notifyTo = mock[ExceptionListenerInside]
      val exceptionHandler = new KafkaStreamsUncaughtExceptionHandler(List(notifyTo.listenException))
      exceptionHandler.uncaughtException(Thread.currentThread(), new RuntimeException("This is expected"))
      verify(notifyTo).listenException(any[KafkaStreamsUncaughtException])
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
