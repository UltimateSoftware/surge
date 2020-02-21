// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams

import java.util.Properties

import org.apache.kafka.streams.{ StreamsConfig, Topology, TopologyTestDriver }

trait KafkaStreamsTestHelpers {
  private val props = new Properties()
  props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test")
  props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "test:1234")

  def withTopologyTestDriver(topology: Topology)(testCode: TopologyTestDriver ⇒ Any) {
    val testDriver = new TopologyTestDriver(topology, props)
    try {
      testCode(testDriver)
    } finally {
      testDriver.close()
    }
  }
}
