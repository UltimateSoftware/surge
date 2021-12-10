// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package docs.command

import io.opentelemetry.sdk.resources.Resource
import surge.core._
import surge.kafka.KafkaTopic
import surge.scaladsl.command.{ AggregateCommandModel, SurgeCommandBusinessLogic }

import java.util.UUID

// #surge_model_class
// format: off
object BankAccountSurgeModelWithHoneycomb
  extends SurgeCommandBusinessLogic[UUID, BankAccount, BankAccountCommand, BankAccountEvent] {

  import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator
  import io.opentelemetry.context.propagation.ContextPropagators
  import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporter
  import io.opentelemetry.sdk.OpenTelemetrySdk
  import io.opentelemetry.sdk.trace.SdkTracerProvider
  import io.opentelemetry.sdk.trace.`export`.BatchSpanProcessor

  override val openTelemetry = {

    val sdkTracerProvider = SdkTracerProvider.builder()
      .addSpanProcessor(BatchSpanProcessor.builder(OtlpGrpcSpanExporter.builder()
        .setEndpoint("https://api.honeycomb.io")
        .addHeader("x-honeycomb-dataset", "data-set-name")
        .addHeader("x-honeycomb-team", "YOUR_API_KEY").build()).build())
      .setResource(Resource.builder().put("service.name", "bank").build())
      .build()

    val openTelemetry: OpenTelemetrySdk = OpenTelemetrySdk.builder()
      .setTracerProvider(sdkTracerProvider)
      .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
      .buildAndRegisterGlobal()

    openTelemetry
  }



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
