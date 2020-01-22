// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.kafka.streams

import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream._

import scala.language.implicitConversions

/**
 * These come from a lightbend library - https://github.com/lightbend/kafka-streams-scala
 * and are just implicit conversions provided as library functions for convenience.
 * We only needed a few things from there and decided to pull these in manually rather
 * than pulling in the whole library for just a couple things.
 */

object ImplicitConversions {

  implicit def tuple2ToKeyValue[K, V](tuple: (K, V)): KeyValue[K, V] = new KeyValue(tuple._1, tuple._2)

  implicit def consumedFromSerde[K, V](implicit keySerde: Serde[K], valueSerde: Serde[V]): Consumed[K, V] =
    Consumed.`with`(keySerde, valueSerde)

  implicit def producedFromSerde[K, V](implicit keySerde: Serde[K], valueSerde: Serde[V]): Produced[K, V] =
    Produced.`with`(keySerde, valueSerde)

  implicit def groupedFromSerde[K, V](implicit keySerde: Serde[K], valueSerde: Serde[V]): Grouped[K, V] =
    Grouped.`with`(keySerde, valueSerde)

  implicit def joinedFromKVOSerde[K, V, VO](implicit keySerde: Serde[K], valueSerde: Serde[V],
    otherValueSerde: Serde[VO]): Joined[K, V, VO] =
    Joined.`with`(keySerde, valueSerde, otherValueSerde)
}
