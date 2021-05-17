// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.kafka.streams

import java.util.Properties

import org.apache.kafka.streams.{ StreamsConfig, Topology, TopologyTestDriver }

trait KafkaStreamsTestHelpers {

  def withTopologyTestDriver(topology: Topology)(testCode: TopologyTestDriver => Any): Unit = {
    val props = new Properties()
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test")
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "test:1234")

    withTopologyTestDriver(topology, props)(testCode)
  }
  def withTopologyTestDriver(topology: Topology, props: Properties)(testCode: TopologyTestDriver => Any): Unit = {
    val testDriver = new TopologyTestDriver(topology, props)
    try {
      testCode(testDriver)
    } finally {
      testDriver.close()
    }
  }
}
