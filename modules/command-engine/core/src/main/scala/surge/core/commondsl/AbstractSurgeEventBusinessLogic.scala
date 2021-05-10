// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.core.commondsl

import com.typesafe.config.Config
import io.opentracing.Tracer
import io.opentracing.noop.NoopTracerFactory
import surge.core.event.{ AggregateEventModelCoreTrait, SurgeEventKafkaConfig }
import surge.core.{ SurgeAggregateFormatting, SurgeEventWriteFormatting }
import surge.kafka.KafkaTopic
import surge.metrics.Metrics

abstract class AbstractSurgeEventBusinessLogic[AggId, Agg, Event](private val config: Config) {

  def aggregateName: String

  def stateTopic: KafkaTopic

  def aggregateFormatting: SurgeAggregateFormatting[Agg]

  def eventWriteFormatting: SurgeEventWriteFormatting[Event]

  protected[surge] def aggregateValidatorLambda: (String, Array[Byte], Option[Array[Byte]]) => Boolean

  def aggregateIdToString(aggId: AggId): String = aggId.toString

  def metrics: Metrics = Metrics.globalMetricRegistry

  def tracer: Tracer = NoopTracerFactory.create()

  def kafkaConfig: SurgeEventKafkaConfig = SurgeEventKafkaConfig(
    stateTopic = stateTopic,
    publishStateOnly = true,
    streamsApplicationId = streamsApplicationId,
    clientId = streamsClientId,
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

  def eventModel: AggregateEventModelCoreTrait[Agg, Event]

}