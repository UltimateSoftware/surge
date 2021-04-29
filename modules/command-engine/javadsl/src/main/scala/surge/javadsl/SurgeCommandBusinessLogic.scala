// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.javadsl

import java.util.Optional

import com.typesafe.config.ConfigFactory
import io.opentracing.Tracer
import io.opentracing.noop.NoopTracerFactory
import surge.core.{ SurgeAggregateReadFormatting, SurgeCommandKafkaConfig, SurgeWriteFormatting }
import surge.kafka.KafkaTopic
import surge.metrics.Metrics

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

  def aggregateIdToString(aggId: AggId): String = aggId.toString

  def aggregateValidator(key: String, aggJson: Array[Byte], prevAggJsonOpt: Optional[Array[Byte]]): Boolean = true

  def metrics: Metrics = Metrics.globalMetricRegistry

  def tracer: Tracer = NoopTracerFactory.create()

  private def kafkaConfig = SurgeCommandKafkaConfig(stateTopic = stateTopic, eventsTopic = eventsTopic,
    publishStateOnly = publishStateOnly, streamsApplicationId = streamsApplicationId, clientId = streamsClientId,
    transactionalIdPrefix = transactionalIdPrefix)

  def consumerGroupBase: String = {
    val environment = config.getString("app.environment")
    s"$aggregateName-$environment-command"
  }

  /**
   * A unique identifier for this application. For scaling, different instances of the same application should have the same application id.
   *
   * It should be noted that two engines with the same streamsApplicationId within a Kafka cluster will form an application cluster. It is therefore very
   * important to ensure that the application environment (development, production, etc...) ends up in the application id somewhere to ensure different
   * environments remain isolated. It is also recommended to add a unique name for your particular aggregate to the application id as well so that different
   * Surge services within the same environment are also completely isolated.
   */
  def streamsApplicationId: String = {
    val environment = config.getString("app.environment")
    s"$consumerGroupBase-$aggregateName-$environment"
  }

  def streamsClientId: String = ""

  def transactionalIdPrefix: String = "surge-transactional-event-producer-partition"

}

object SurgeCommandBusinessLogic {
  def toCore[Agg, Command, Event](
    businessLogic: SurgeCommandBusinessLogic[_, Agg, Command, Event]): surge.core.SurgeCommandBusinessLogic[Agg, Command, Event] = {
    new surge.core.SurgeCommandBusinessLogic[Agg, Command, Event](
      aggregateName = businessLogic.aggregateName,
      kafka = businessLogic.kafkaConfig,
      model = businessLogic.commandModel.toCore,
      writeFormatting = businessLogic.writeFormatting,
      readFormatting = businessLogic.readFormatting,
      aggregateValidator = (key, agg, prevAgg) => businessLogic.aggregateValidator(key, agg, prevAgg.asJava),
      metrics = businessLogic.metrics,
      tracer = businessLogic.tracer)
  }
}
