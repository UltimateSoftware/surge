// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.kafka.streams

import java.util

import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.processor.{ ProcessorContext, StateStore }
import org.apache.kafka.streams.state.{ KeyValueBytesStoreSupplier, KeyValueIterator, KeyValueStore }

import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.control.NoStackTrace

class ExpectedTestException extends RuntimeException("This is expected") with NoStackTrace

class SingleExceptionThrowingKeyValueStoreSupplier(val name: String) extends KeyValueBytesStoreSupplier {
  override def get(): SingleExceptionThrowingKeyValueStore = new SingleExceptionThrowingKeyValueStore(name)
  override def metricsScope(): String = "SingleExceptionThrowingKeyValueStore"
}

class SingleExceptionThrowingKeyValueStore(val name: String) extends KeyValueStore[Bytes, Array[Byte]] {
  private var exceptionCount: Int = 0
  private val stateMap: mutable.Map[Bytes, Array[Byte]] = mutable.Map.empty

  private def maybeThrowException(): Unit = {
    if (exceptionCount < 1) {
      exceptionCount += 1
      throw new ExpectedTestException
    }
  }

  override def put(key: Bytes, value: Array[Byte]): Unit = {
    maybeThrowException()
    stateMap.put(key, value)
  }
  override def putIfAbsent(key: Bytes, value: Array[Byte]): Array[Byte] = {
    maybeThrowException()
    val prev = stateMap.get(key)
    if (prev.isEmpty) {
      stateMap.put(key, value)
    }
    prev.orNull
  }
  override def putAll(entries: util.List[KeyValue[Bytes, Array[Byte]]]): Unit = {
    maybeThrowException()
    entries.asScala.foreach(kv => put(kv.key, kv.value))
  }
  override def delete(key: Bytes): Array[Byte] = {
    maybeThrowException()
    stateMap.remove(key).orNull
  }

  override def get(key: Bytes): Array[Byte] = {
    maybeThrowException()
    stateMap.get(key).orNull
  }

  private class InMemoryKVIterator(entries: mutable.Map[Bytes, Array[Byte]]) extends KeyValueIterator[Bytes, Array[Byte]] {
    private val iterator = entries.iterator
    override def close(): Unit = {}
    override def peekNextKey(): Bytes = throw new UnsupportedOperationException
    override def hasNext: Boolean = iterator.hasNext
    override def next(): KeyValue[Bytes, Array[Byte]] = {
      val (key, value) = iterator.next()
      new KeyValue[Bytes, Array[Byte]](key, value)
    }
  }

  override def range(from: Bytes, to: Bytes): KeyValueIterator[Bytes, Array[Byte]] = {
    maybeThrowException()
    val filtered = stateMap.filter { case (key, _) => key.compareTo(from) >= 0 && key.compareTo(to) <= 0 }
    new InMemoryKVIterator(filtered)
  }

  override def all(): KeyValueIterator[Bytes, Array[Byte]] = {
    maybeThrowException()
    new InMemoryKVIterator(stateMap)
  }

  override def approximateNumEntries(): Long = stateMap.size

  override def init(context: ProcessorContext, root: StateStore): Unit = {
    context.register(root, (key: Array[Byte], value: Array[Byte]) => stateMap.put(new Bytes(key), value))
  }
  override def flush(): Unit = {}
  override def close(): Unit = {}
  override def persistent(): Boolean = false
  override def isOpen: Boolean = true
}
