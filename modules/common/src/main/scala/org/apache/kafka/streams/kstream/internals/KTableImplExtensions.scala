// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

package org.apache.kafka.streams.kstream.internals

import java.util

import org.apache.kafka.common.serialization.{ Deserializer, Serde, Serializer }
import org.apache.kafka.streams.kstream.internals.graph.{ ProcessorGraphNode, ProcessorParameters }
import org.apache.kafka.streams.scala.kstream.KStream

// Using this to be able to build out a table directly from a compacted Kafka topic with the streams builder DSL
// so we don't need an explicit changelog topic, but can also stream old values.
object KTableImplExtensions {
  def serdeForChanged[V](implicit valSerde: Serde[V]): Serde[Change[V]] = new Serde[Change[V]] {
    private val changeSerializer = new ChangedSerializer[V](valSerde.serializer())
    private val changeDeserializer = new ChangedDeserializer[V](valSerde.deserializer())

    override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {
      changeSerializer.configure(configs, isKey)
      changeDeserializer.configure(configs, isKey)
    }
    override def close(): Unit = {
      changeSerializer.close()
      changeDeserializer.close()
    }
    override def serializer(): Serializer[Change[V]] = changeSerializer
    override def deserializer(): Deserializer[Change[V]] = changeDeserializer
  }

  implicit class KTableImplExtension[K, S, V](ktableImpl: KTableImpl[K, S, V]) {
    def sendOldValues(): Unit = ktableImpl.enableSendingOldValues()
    def getName: String = ktableImpl.name

    def toStreamWithChanges: KStream[K, Change[V]] = {
      val name = ktableImpl.builder.newProcessorName("KTABLE-TOSTREAM-")

      val kStreamMapValues = new KStreamMapValues[K, Change[V], Change[V]]((key, change) ⇒ change)

      val toStreamNode = new ProcessorGraphNode[K, Change[V]](name, new ProcessorParameters(kStreamMapValues, name))

      ktableImpl.builder.addGraphNode(ktableImpl.streamsGraphNode, toStreamNode)

      // we can inherit parent key and value serde
      val impl = new KStreamImpl[K, Change[V]](name, ktableImpl.keySerde, serdeForChanged(ktableImpl.valSerde), ktableImpl.sourceNodes,
        false, toStreamNode, ktableImpl.builder)

      new KStream(impl)
    }
  }
}
