// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package docs.consumer

import akka.Done
import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import io.opentelemetry.api.trace.Tracer
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator
import io.opentelemetry.context.propagation.ContextPropagators
import io.opentelemetry.exporter.jaeger.JaegerGrpcSpanExporter
import io.opentelemetry.sdk.OpenTelemetrySdk
import io.opentelemetry.sdk.resources.Resource
import io.opentelemetry.sdk.trace.SdkTracerProvider
import io.opentelemetry.sdk.trace.`export`.BatchSpanProcessor
import org.apache.kafka.common.serialization.Deserializer
import surge.core._
import surge.internal.akka.kafka.AkkaKafkaConsumer
import surge.kafka.KafkaTopic
import surge.kafka.streams.DefaultSerdes
import surge.streams.{ EventSink, KafkaEventSource }

import scala.concurrent.Future

class ConsumerSpec {

  // format: off
  // #consumer

  val openTelemetry: OpenTelemetrySdk = {

    val exporter =
      JaegerGrpcSpanExporter.builder()
        .setEndpoint("http://localhost:14250")
        .build()

    val sdkTracerProvider = SdkTracerProvider.builder()
      .addSpanProcessor(BatchSpanProcessor.builder(exporter).build())
      .setResource(Resource.builder().put("service.name", "bank").build())
      .build()

    val openTelemetry: OpenTelemetrySdk = OpenTelemetrySdk.builder()
      .setTracerProvider(sdkTracerProvider)
      .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
      .buildAndRegisterGlobal()

    openTelemetry
  }

  implicit val myTracer = openTelemetry.getTracer("MyApp", "0.1")

  implicit val system = ActorSystem()

  val kafkaEventSource: KafkaEventSource[String] = new KafkaEventSource[String] {

    override def baseEventName: String = ???

    override def kafkaTopic: KafkaTopic = KafkaTopic(name = ???)

    override def formatting: SurgeEventReadFormatting[String] = bytes => new String(bytes)

    override def actorSystem: ActorSystem = system

    override def tracer: Tracer = myTracer

  }

  private implicit val stringDeserializer: Deserializer[String] = DefaultSerdes.stringSerde.deserializer()
  private implicit val byteArrayDeserializer: Deserializer[Array[Byte]] = DefaultSerdes.byteArraySerde.deserializer()

  val consumerSettings =
    new AkkaKafkaConsumer(ConfigFactory.load())
      .consumerSettings[String, Array[Byte]](system,
        groupId = "group-id-1",
        brokers = "127.0.0.1:9092",
        autoOffsetReset = "earliest")

  val eventSink: EventSink[String] = new EventSink[String] {
    override def handleEvent(key: String, event: String, headers: Map[String, Array[Byte]]): Future[Any] = {
      // do something with the event (i.e. save to ElasticSearch)
      Future.successful(Done)
    }

    override def sinkName: String = "ElasticSearch"

    override def partitionBy(key: String, event: String, headers: Map[String, Array[Byte]]): String =
      event
  }

  kafkaEventSource.to(consumerSettings)(eventSink, autoStart = true)

  // #consumer
  // format: on

}
