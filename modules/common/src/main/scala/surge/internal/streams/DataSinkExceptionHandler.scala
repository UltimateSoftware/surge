// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.streams

import org.slf4j.LoggerFactory
import surge.metrics.{ MetricInfo, Metrics }
import surge.streams.{ DataSinkExceptionHandler, EventPlusStreamMeta }

abstract class CommonDataSinkExceptionHandler[K, V] extends DataSinkExceptionHandler[K, V] {
  private val log = LoggerFactory.getLogger(getClass)
  def formatMeta[Meta](epm: EventPlusStreamMeta[K, V, Meta]): String
  def logMessage[Meta](epm: EventPlusStreamMeta[K, V, Meta], exception: Throwable): Unit = {
    log.error(
      s"An exception was thrown by the event handler for message with metadata [${formatMeta(epm)}]! " +
        s"The stream will restart and the message will be retried.",
      exception)
  }
}
class DefaultDataSinkExceptionHandlerWithSupport[K, V](
    metrics: Metrics,
    metricTags: Map[String, String],
    metaFormatter: EventPlusStreamMeta[K, V, Any] => String)
    extends CommonDataSinkExceptionHandler[K, V] {
  private val eventExceptionMetric =
    metrics.rate(MetricInfo(name = "surge.event.handler.exception.rate", description = "rate of exceptions caught wile handling and event", tags = metricTags))

  override def handleException[Meta](epm: EventPlusStreamMeta[K, V, Meta], exception: Throwable): Unit = {
    eventExceptionMetric.mark()
    logMessage(epm, exception)
    throw exception
  }

  override def formatMeta[Meta](epm: EventPlusStreamMeta[K, V, Meta]): String = metaFormatter.apply(epm)
}

class DefaultDataSinkExceptionHandler[K, V] extends CommonDataSinkExceptionHandler[K, V] {
  private val log = LoggerFactory.getLogger(getClass)

  override def handleException[Meta](epm: EventPlusStreamMeta[K, V, Meta], exception: Throwable): Unit = {
    logMessage(epm, exception)
    throw exception
  }

  override def formatMeta[Meta](epm: EventPlusStreamMeta[K, V, Meta]): String = epm.streamMeta.toString
}
