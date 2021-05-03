// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.kafka.streams

import org.apache.kafka.streams.state.ReadOnlyKeyValueStore

import scala.concurrent.{ ExecutionContext, Future }
import scala.jdk.CollectionConverters._

/**
 * Asynchronous wrapper for a Kafka Streams ReadOnlyKeyValueStore. Just wraps calls the underlying key value store in a scala Future run in a separate thread
 * pool. This helps other calling code be more non-blocking and greatly improves efficiency.
 *
 * @param underlying
 *   The underlying Kafka Streams ReadOnlyKeyValueStore used to fetch data from.
 * @tparam Key
 *   Generic key type for the Key/Value store
 * @tparam Value
 *   Generic value type for the Key/Value store
 */
class KafkaStreamsKeyValueStore[Key, Value](underlying: ReadOnlyKeyValueStore[Key, Value]) {
  private implicit val executionContext: ExecutionContext = ThreadPools.ioBoundContext

  def get(key: Key): Future[Option[Value]] = Future {
    Option(underlying.get(key))
  }

  def all(): Future[List[(Key, Value)]] = Future {
    val iterator = underlying.all()

    val result = iterator.asScala.map(kv => kv.key -> kv.value).toList

    // Wrap this close() call in a future, even though we're already in a future, because it slows things down otherwise
    Future {
      iterator.close()
    }

    result
  }

  def range(from: Key, to: Key): Future[List[(Key, Value)]] = Future {
    val iterator = underlying.range(from, to)
    val result = iterator.asScala.map(kv => kv.key -> kv.value).toList

    // Wrap this close() call in a future, even though we're already in a future, because it slows things down otherwise
    Future {
      iterator.close()
    }

    result
  }

  def allValues(): Future[List[Value]] = Future {
    val iterator = underlying.all()

    val result = iterator.asScala.map(_.value).toList

    // Wrap this close() call in a future, even though we're already in a future, because it slows things down otherwise
    Future {
      iterator.close()
    }

    result
  }

  def approximateNumEntries(): Future[Long] = Future {
    underlying.approximateNumEntries()
  }
}
