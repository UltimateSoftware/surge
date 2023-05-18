// Copyright © 2017-2023 UKG Inc. <https://www.ukg.com>

package surge.kafka.streams

import org.apache.kafka.streams.errors.InvalidStateStoreException
import org.slf4j.LoggerFactory

import scala.concurrent.{ ExecutionContext, Future }

class SurgeAggregateStore(storeName: String, aggregateQueryableStateStore: KafkaStreamsKeyValueStore[String, Array[Byte]])(implicit ec: ExecutionContext) {
  private val log = LoggerFactory.getLogger(getClass)

  def getSubstatesForAggregate(aggregateId: String): Future[List[(String, Array[Byte])]] = {
    aggregateQueryableStateStore
      .range(aggregateId, s"$aggregateId:~")
      .map { result =>
        result.filter { case (key, _) =>
          val keyBeforeColon = key.takeWhile(_ != ':')
          keyBeforeColon == aggregateId
        }
      }
      .recoverWith {
        case err: InvalidStateStoreException =>
          handleInvalidStateStore(err)
        case err: Throwable =>
          log.error(s"State store $storeName threw an unexpected error", err)
          Future.failed(err)
      }
  }

  def getAggregateBytes(aggregateId: String): Future[Option[Array[Byte]]] = {
    aggregateQueryableStateStore.get(aggregateId).recoverWith {
      case err: InvalidStateStoreException =>
        handleInvalidStateStore(err)
      case err: Throwable =>
        log.error(s"State store $storeName threw an unexpected error", err)
        Future.failed(err)
    }
  }

  private def handleInvalidStateStore[T](err: InvalidStateStoreException): Future[T] = {
    log.warn(
      s"State store $storeName saw InvalidStateStoreException: ${err.getMessage}. " +
        s"This error is typically caused by a consumer group rebalance.")
    Future.failed(err)
  }
}
