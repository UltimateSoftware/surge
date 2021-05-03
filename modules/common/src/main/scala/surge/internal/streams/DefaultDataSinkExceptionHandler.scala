// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.streams

import org.slf4j.LoggerFactory
import surge.streams.DataSinkExceptionHandler

class DefaultDataSinkExceptionHandler[K, V]() extends DataSinkExceptionHandler[K, V] {
  private val log = LoggerFactory.getLogger(getClass)
  override def handleException[Meta](key: K, value: V, streamMeta: Meta, exception: Throwable): Unit = {
    log.error(
      s"An exception was thrown by the event handler for message with metadata [$streamMeta]! " +
        s"The stream will restart and the message will be retried.",
      exception)
    throw exception
  }
}
