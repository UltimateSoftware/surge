// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.kafka.streams

import org.apache.kafka.streams.kstream.internals.Change
import org.apache.kafka.streams.kstream.{ ValueTransformerWithKey, ValueTransformerWithKeySupplier }
import org.apache.kafka.streams.processor.ProcessorContext
import org.slf4j.{ Logger, LoggerFactory }

class ValidationProcessor[Agg](aggregateName: String, aggregateValidator: (String, Array[Byte], Option[Array[Byte]]) => Boolean) {

  private val log: Logger = LoggerFactory.getLogger(getClass)

  val supplier: ValueTransformerWithKeySupplier[String, Change[Array[Byte]], KafkaPartitionMetadata] = { () =>
    new StateProcessorImpl
  }

  private class StateProcessorImpl extends ValueTransformerWithKey[String, Change[Array[Byte]], KafkaPartitionMetadata] {
    private var context: ProcessorContext = _

    override def init(context: ProcessorContext): Unit = {
      this.context = context
    }

    override def transform(readOnlyKey: String, value: Change[Array[Byte]]): KafkaPartitionMetadata = {
      val stateMeta = KafkaPartitionMetadata.fromContext(context, readOnlyKey)
      val currentStateOpt: Option[Array[Byte]] = Option(value.oldValue)

      val successfullyValidated = aggregateValidator(readOnlyKey, value.newValue, currentStateOpt)
      if (!successfullyValidated) {
        log.warn(s"Validation for aggregate $aggregateName failed for $stateMeta")
      }

      stateMeta
    }

    override def close(): Unit = {}
  }
}
