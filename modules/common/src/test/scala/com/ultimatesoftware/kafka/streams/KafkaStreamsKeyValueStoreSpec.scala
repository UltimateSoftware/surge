// Copyright © 2018-2020 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams

import java.util.Properties

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.processor.{ MockProcessorContext, StateRestoreCallback }
import org.apache.kafka.streams.state.Stores
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatestplus.mockito.MockitoSugar

class KafkaStreamsKeyValueStoreSpec extends AsyncWordSpec with BeforeAndAfterAll with Matchers with MockitoSugar {
  private val props = new Properties()
  props.put(StreamsConfig.APPLICATION_ID_CONFIG, "unit-test")
  props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "")
  private val context = new MockProcessorContext(props)
  private val keyValueStore =
    Stores.keyValueStoreBuilder(
      Stores.inMemoryKeyValueStore("myStore"),
      Serdes.String(),
      Serdes.String()).withLoggingDisabled() // Changelog is not supported by MockProcessorContext.
      .build()

  override def beforeAll(): Unit = {
    val dataMap = Map(
      "Dwight" -> "Dwight",
      "Jim" -> "Jim",
      "Kevin" -> "Kevin",
      "Michael" -> "Michael",
      "Pam" -> "Pam")

    keyValueStore.init(context, keyValueStore)
    context.register(keyValueStore, mock[StateRestoreCallback])

    dataMap.foreach(kv ⇒ keyValueStore.put(kv._1, kv._2))
  }

  "KafkaStreamsKeyValueStore" should {
    "Properly wrap get single" in {
      val store = new KafkaStreamsKeyValueStore(keyValueStore)
      for {
        maybePam ← store.get("Pam")
        maybeOscar ← store.get("Oscar")
      } yield {
        maybePam shouldEqual Some("Pam")
        maybeOscar shouldEqual None
      }
    }

    "Properly wrap get all" in {
      val store = new KafkaStreamsKeyValueStore(keyValueStore)
      for {
        all ← store.all()
      } yield {
        all should have size 5
        all should contain("Dwight" -> "Dwight")
        all should contain("Jim" -> "Jim")
        all should contain("Kevin" -> "Kevin")
        all should contain("Michael" -> "Michael")
        all should contain("Pam" -> "Pam")
      }
    }

    "Properly wrap get all values" in {
      val store = new KafkaStreamsKeyValueStore(keyValueStore)
      for {
        values ← store.allValues()
      } yield {
        values should have size 5
        values should contain("Dwight")
        values should contain("Jim")
        values should contain("Kevin")
        values should contain("Michael")
        values should contain("Pam")
      }
    }

    "Properly wrap range query" in {
      val store = new KafkaStreamsKeyValueStore(keyValueStore)
      for {
        rangeResult ← store.range("J", "M")
      } yield {
        rangeResult should have size 2
        rangeResult should contain("Jim" -> "Jim")
        rangeResult should contain("Kevin" -> "Kevin")
      }
    }

    "Properly approximate the number of entries in the state store" in {
      val store = new KafkaStreamsKeyValueStore(keyValueStore)
      store.approximateNumEntries().map(_ shouldEqual 5L)
    }
  }
}
