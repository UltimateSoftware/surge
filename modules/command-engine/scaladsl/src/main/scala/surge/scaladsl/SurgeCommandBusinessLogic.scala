// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.scaladsl

import com.typesafe.config.ConfigFactory
import surge.core.{ SurgeAggregateReadFormatting, SurgeCommandKafkaConfig, SurgeWriteFormatting }
import surge.metrics.{ MetricsProvider, MetricsPublisher, NoOpMetricsProvider, NoOpsMetricsPublisher }
import surge.scala.core.kafka.KafkaTopic
import surge.scala.core.validations.AsyncCommandValidator
import surge.scala.oss.domain.AggregateCommandModel

import scala.concurrent.duration._

trait SurgeCommandBusinessLogic[AggId, Agg, Command, Event] {

  private val config = ConfigFactory.load()

  def aggregateName: String

  def stateTopic: KafkaTopic

  def eventsTopic: KafkaTopic
  def publishStateOnly: Boolean

  def commandModel: AggregateCommandModel[Agg, Command, Event]

  def readFormatting: SurgeAggregateReadFormatting[Agg]

  def writeFormatting: SurgeWriteFormatting[Agg, Event]

  def commandValidator: AsyncCommandValidator[Command, Agg]

  def aggregateValidator: (String, Array[Byte], Option[Array[Byte]]) => Boolean = { (_, _, _) => true }

  // Defaults to noops publishing (for now) and 30 second interval on metrics snapshots
  // These can be overridden in the derived applications
  def metricsPublisher: MetricsPublisher = NoOpsMetricsPublisher

  def metricsInterval: FiniteDuration = 30.seconds

  def metricsProvider: MetricsProvider = NoOpMetricsProvider

  def aggregateIdToString: AggId => String

  def consumerGroupBase: String = {
    val environment = config.getString("app.environment")
    s"$aggregateName-$environment-command"
  }

  def transactionalIdPrefix: String = "surge-transactional-event-producer-partition"

  private def kafkaConfig: SurgeCommandKafkaConfig = SurgeCommandKafkaConfig(
    stateTopic = stateTopic,
    eventsTopic = eventsTopic, publishStateOnly = publishStateOnly)
}

object SurgeCommandBusinessLogic {

  def toCore[Agg, Command, Event](
    businessLogic: SurgeCommandBusinessLogic[_, Agg, Command, Event]): surge.core.SurgeCommandBusinessLogic[Agg, Command, Event] = {
    new surge.core.SurgeCommandBusinessLogic[Agg, Command, Event](
      aggregateName = businessLogic.aggregateName,
      kafka = businessLogic.kafkaConfig,
      model = businessLogic.commandModel,
      readFormatting = businessLogic.readFormatting,
      writeFormatting = businessLogic.writeFormatting,
      commandValidator = businessLogic.commandValidator,
      aggregateValidator = businessLogic.aggregateValidator,
      metricsProvider = businessLogic.metricsProvider,
      metricsPublisher = businessLogic.metricsPublisher,
      metricsInterval = businessLogic.metricsInterval,
      consumerGroup = businessLogic.consumerGroupBase,
      transactionalIdPrefix = businessLogic.transactionalIdPrefix)
  }
}
