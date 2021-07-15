// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package docs.command

import io.jaegertracing.Configuration
import io.jaegertracing.internal.JaegerTracer
import io.opentracing.Tracer
import play.api.libs.json.Json
import surge.core._
import surge.kafka.KafkaTopic
import surge.scaladsl.command.{ AggregateCommandModel, SurgeCommandBusinessLogic }

import java.util.UUID

// #surge_model_class
// format: off
object BankAccountSurgeModelWithTracer
  extends SurgeCommandBusinessLogic[UUID, BankAccount, BankAccountCommand, BankAccountEvent] {

  import io.jaegertracing.Configuration
  import io.jaegertracing.internal.JaegerTracer
  import io.opentracing.Tracer

  def createJaegerTracer(): JaegerTracer = {
    val samplerConfig = Configuration.SamplerConfiguration.fromEnv()
      .withType("const")
      .withParam(1)

    val reporterConfig = Configuration.ReporterConfiguration.fromEnv()
      .withLogSpans(true)

    val builder = new Configuration("bank-account-service")
      .withSampler(samplerConfig)
      .withReporter(reporterConfig)
      .getTracerBuilder

    builder.build()
  }

  override def tracer: Tracer = createJaegerTracer()

  override def commandModel: AggregateCommandModel[BankAccount, BankAccountCommand, BankAccountEvent]
   = BankAccountCommandModel

  override def aggregateName: String = "bank-account"

  override def stateTopic: KafkaTopic = KafkaTopic("bank-account-state")

  override def eventsTopic: KafkaTopic = KafkaTopic("bank-account-events")

  override def aggregateReadFormatting: SurgeAggregateReadFormatting[BankAccount] = ???

  override def aggregateWriteFormatting: SurgeAggregateWriteFormatting[BankAccount] = ???

  override def eventWriteFormatting: SurgeEventWriteFormatting[BankAccountEvent] = ???

}
// #surge_model_class
// format: on
