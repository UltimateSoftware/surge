// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams

import org.apache.kafka.streams.scala.kstream.KStream

trait KafkaPartitionMetadataHandler {
  def processPartitionMetadata(stream: KStream[String, KafkaPartitionMetadata]): Unit = {}
  def initialize(): Unit = {}
  def start(): Unit = {}
  def stop(): Unit = {}
}

object NoOpKafkaPartitionMetadataHandler extends KafkaPartitionMetadataHandler
