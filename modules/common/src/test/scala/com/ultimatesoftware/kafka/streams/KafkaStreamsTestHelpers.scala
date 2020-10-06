// Copyright © 2018-2020 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams

import java.util.Properties

import org.apache.kafka.streams.{ StreamsConfig, Topology, TopologyTestDriver }

trait KafkaStreamsTestHelpers {

  def withTopologyTestDriver(topology: Topology)(testCode: TopologyTestDriver ⇒ Any) {
    val props = new Properties()
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test")
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "test:1234")

    withTopologyTestDriver(topology, props)(testCode)
  }
  def withTopologyTestDriver(topology: Topology, props: Properties)(testCode: TopologyTestDriver ⇒ Any) {
    val testDriver = new TopologyTestDriver(topology, props)
    try {
      testCode(testDriver)
    } finally {
      testDriver.close()
    }
  }
}
