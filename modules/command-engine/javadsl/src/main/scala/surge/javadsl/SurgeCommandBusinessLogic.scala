// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.javadsl

import java.util.Optional

import com.typesafe.config.ConfigFactory
import com.ultimatesoftware.scala.core.monitoring.metrics.{ MetricsProvider, MetricsPublisher, NoOpMetricsProvider, NoOpsMetricsPublisher }
import surge.core.{ SurgeAggregateReadFormatting, SurgeCommandKafkaConfig, SurgeWriteFormatting }
import surge.scala.core.kafka.KafkaTopic
import surge.scala.core.validations.AsyncCommandValidator
import surge.scala.oss.domain.AggregateCommandModel

import scala.compat.java8.OptionConverters._
import scala.concurrent.duration._

abstract class SurgeCommandBusinessLogic[AggId, Agg, Command, Event] {

  private val config = ConfigFactory.load()

  def aggregateName: String

  def stateTopic: KafkaTopic

  def eventsTopic: KafkaTopic

  def commandModel: AggregateCommandModel[Agg, Command, Event]

  def readFormatting: SurgeAggregateReadFormatting[Agg]

  def writeFormatting: SurgeWriteFormatting[Agg, Event]

  def commandValidator: AsyncCommandValidator[Command, Agg]

  def aggregateIdToString(aggId: AggId): String = aggId.toString

  def aggregateValidator(key: String, aggJson: Array[Byte], prevAggJsonOpt: Optional[Array[Byte]]): Boolean = true

  // Defaults to noops publishing (for now) and 30 second interval on metrics snapshots
  // These can be overridden in the derived applications
  def metricsPublisher: MetricsPublisher = NoOpsMetricsPublisher

  def metricsInterval: FiniteDuration = 30.seconds

  def metricsProvider: MetricsProvider = NoOpMetricsProvider

  private def kafkaConfig = SurgeCommandKafkaConfig(stateTopic = stateTopic, eventsTopic = eventsTopic)

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
      aggregateValidator = (key, agg, prevAgg) ⇒ businessLogic.aggregateValidator(key, agg, prevAgg.asJava),
      metricsProvider = businessLogic.metricsProvider,
      metricsPublisher = businessLogic.metricsPublisher,
      metricsInterval = businessLogic.metricsInterval,
      consumerGroup = businessLogic.consumerGroupBase,
      transactionalIdPrefix = businessLogic.transactionalIdPrefix)
  }
}
