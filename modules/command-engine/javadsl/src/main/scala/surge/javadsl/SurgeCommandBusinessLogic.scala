// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.javadsl

import java.util.Optional

import com.typesafe.config.ConfigFactory
import surge.core.{ SurgeAggregateReadFormatting, SurgeCommandKafkaConfig, SurgeWriteFormatting }
import surge.metrics.Metrics
import surge.scala.core.kafka.KafkaTopic
import surge.scala.core.validations.AsyncCommandValidator
import surge.scala.oss.domain.AggregateCommandModel

import scala.compat.java8.OptionConverters._

abstract class SurgeCommandBusinessLogic[AggId, Agg, Command, Event] {

  private val config = ConfigFactory.load()

  def aggregateName: String

  def stateTopic: KafkaTopic

  def eventsTopic: KafkaTopic
  def publishStateOnly: Boolean = false

  def commandModel: AggregateCommandModel[Agg, Command, Event]

  def readFormatting: SurgeAggregateReadFormatting[Agg]

  def writeFormatting: SurgeWriteFormatting[Agg, Event]

  def commandValidator: AsyncCommandValidator[Command, Agg]

  def aggregateIdToString(aggId: AggId): String = aggId.toString

  def aggregateValidator(key: String, aggJson: Array[Byte], prevAggJsonOpt: Optional[Array[Byte]]): Boolean = true

  def metrics: Metrics = Metrics.globalMetricRegistry

  private def kafkaConfig = SurgeCommandKafkaConfig(stateTopic = stateTopic, eventsTopic = eventsTopic, publishStateOnly = publishStateOnly)

  def consumerGroupBase: String = {
    val environment = config.getString("app.environment")
    s"$aggregateName-$environment-command"
  }

  def transactionalIdPrefix: String = "surge-transactional-event-producer-partition"

}

object SurgeCommandBusinessLogic {
  def toCore[Agg, Command, Event](
    businessLogic: SurgeCommandBusinessLogic[_, Agg, Command, Event]): surge.core.SurgeCommandBusinessLogic[Agg, Command, Event] = {
    new surge.core.SurgeCommandBusinessLogic[Agg, Command, Event](
      aggregateName = businessLogic.aggregateName,
      kafka = businessLogic.kafkaConfig,
      model = businessLogic.commandModel,
      writeFormatting = businessLogic.writeFormatting,
      readFormatting = businessLogic.readFormatting,
      commandValidator = businessLogic.commandValidator,
      aggregateValidator = (key, agg, prevAgg) => businessLogic.aggregateValidator(key, agg, prevAgg.asJava),
      consumerGroup = businessLogic.consumerGroupBase,
      transactionalIdPrefix = businessLogic.transactionalIdPrefix,
      metrics = businessLogic.metrics)
  }
}
