// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.streams

import org.slf4j.{Logger, LoggerFactory}
import surge.metrics.{MetricInfo, Metrics}
import surge.streams.{DataSinkExceptionHandler, EventPlusStreamMeta}

abstract class CommonDataSinkExceptionHandler[K, V] extends DataSinkExceptionHandler[K, V] {
  protected val log: Logger = LoggerFactory.getLogger(getClass)
  def formatMeta[Meta](epm: EventPlusStreamMeta[K, V, Meta]): String
  def logMessage[Meta](epm: EventPlusStreamMeta[K, V, Meta], exception: Throwable): Unit = {
    log.error(
      s"An exception was thrown by the event handler for message with metadata [${formatMeta(epm)}]! " +
        s"The stream will restart and the message will be retried.",
      exception)
  }
}


trait MetaFormatter[K, V] {
  def formatMeta[Meta](epm: EventPlusStreamMeta[K, V, Meta]): String

}
class DefaultDataSinkExceptionHandlerWithSupport[K, V](
    metrics: Metrics,
    metricTags: Map[String, String],
    metaFormatter: MetaFormatter[K,V])
    extends CommonDataSinkExceptionHandler[K, V] {
  private val eventExceptionMetric =
    metrics.rate(MetricInfo(name = "surge.event.handler.exception.rate", description = "rate of exceptions caught while handling and event", tags = metricTags))

  override def handleException[Meta](epm: EventPlusStreamMeta[K, V, Meta], exception: Throwable): Unit = {
    eventExceptionMetric.mark()
    logMessage(epm, exception)
    throw exception
  }

  override def formatMeta[Meta](epm: EventPlusStreamMeta[K, V, Meta]): String = metaFormatter.formatMeta(epm)

}

class DefaultDataSinkExceptionHandler[K, V] extends CommonDataSinkExceptionHandler[K, V] {

  override def handleException[Meta](epm: EventPlusStreamMeta[K, V, Meta], exception: Throwable): Unit = {
    logMessage(epm, exception)
    throw exception
  }

  override def formatMeta[Meta](epm: EventPlusStreamMeta[K, V, Meta]): String = epm.streamMeta.toString
}
