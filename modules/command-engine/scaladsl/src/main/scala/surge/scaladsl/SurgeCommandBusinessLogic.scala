// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.scaladsl

import com.typesafe.config.ConfigFactory
import surge.core.{ SurgeAggregateReadFormatting, SurgeCommandKafkaConfig, SurgeWriteFormatting }
import surge.domain.AggregateCommandModel
import surge.kafka.KafkaTopic
import surge.metrics.Metrics

trait SurgeCommandBusinessLogic[AggId, Agg, Command, Event] {

  private val config = ConfigFactory.load()

  def aggregateName: String

  def stateTopic: KafkaTopic

  def eventsTopic: KafkaTopic
  def publishStateOnly: Boolean = false

  def commandModel: AggregateCommandModel[Agg, Command, Event]

  def readFormatting: SurgeAggregateReadFormatting[Agg]

  def writeFormatting: SurgeWriteFormatting[Agg, Event]

  def aggregateValidator: (String, Array[Byte], Option[Array[Byte]]) => Boolean = { (_, _, _) => true }

  def metrics: Metrics = Metrics.globalMetricRegistry

  def aggregateIdToString: AggId => String

  def consumerGroupBase: String = {
    val environment = config.getString("app.environment")
    s"$aggregateName-$environment-command"
  }

  def streamsClientId: String = ""

  def transactionalIdPrefix: String = "surge-transactional-event-producer-partition"

  private def kafkaConfig = SurgeCommandKafkaConfig(stateTopic = stateTopic, eventsTopic = eventsTopic,
    publishStateOnly = publishStateOnly, consumerGroup = consumerGroupBase, clientId = streamsClientId,
    transactionalIdPrefix = transactionalIdPrefix)
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
      aggregateValidator = businessLogic.aggregateValidator,
      metrics = businessLogic.metrics)
  }
}
