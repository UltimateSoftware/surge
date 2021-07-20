// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.core.commondsl

import com.typesafe.config.{ Config, ConfigFactory }
import io.opentelemetry.api.trace.Tracer
import surge.core.{ SurgeAggregateReadFormatting, SurgeAggregateWriteFormatting }
import surge.internal.tracing.NoopTracerFactory
import surge.kafka.KafkaTopic
import surge.metrics.Metrics

import scala.concurrent.ExecutionContext
import scala.concurrent.ExecutionContext.global

trait SurgeGenericBusinessLogicTrait[AggId, Agg, Command, Rej, Event] {

  protected val config: Config = ConfigFactory.load()

  def aggregateName: String

  def stateTopic: KafkaTopic

  def publishStateOnly: Boolean

  def aggregateReadFormatting: SurgeAggregateReadFormatting[Agg]
  def aggregateWriteFormatting: SurgeAggregateWriteFormatting[Agg]

  protected[surge] def aggregateValidatorLambda: (String, Array[Byte], Option[Array[Byte]]) => Boolean

  protected[surge] def aggregateIdToString(aggId: AggId): String = aggId.toString

  def metrics: Metrics = Metrics.globalMetricRegistry

  def tracer: Tracer = NoopTracerFactory.create()

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

  val executionContext: ExecutionContext = global
}
