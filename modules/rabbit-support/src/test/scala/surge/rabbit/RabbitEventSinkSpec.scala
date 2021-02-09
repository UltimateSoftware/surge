// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.rabbit

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.alpakka.amqp.scaladsl.AmqpSource
import akka.stream.alpakka.amqp._
import akka.stream.scaladsl.{ Keep, Sink, Source }
import akka.testkit.{ TestKit, TestProbe }
import org.mockito.Mockito._
import org.mockito.{ ArgumentCaptor, ArgumentMatchers }
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import surge.core.{ EventPlusStreamMeta, SerializedMessage, SurgeEventWriteFormatting }

import scala.concurrent.duration._

object RabbitEventSinkSpec {
  val SERVICE_PORT: Int = 5672
}

class RabbitEventSinkSpec extends TestKit(ActorSystem("RabbitEventSinkSpec"))
  with EmbeddedRabbit
  with AnyWordSpecLike with Matchers with BeforeAndAfterAll {
  class TestRabbitEventSink(
      val rabbitMqUri: String,
      val queueName: String,
      val probe: TestProbe,
      override val bufferSize: Int = 10,
      override val writeRoute: String,
      override val autoDeclarePlan: Option[AutoDeclarePlan]) extends RabbitEventSink[String] {
    override def formatting: SurgeEventWriteFormatting[String] = (event: String) => SerializedMessage("message", event.getBytes(), Map.empty)

    override def interceptWriteMessage(writeMessage: WriteMessage): WriteMessage = {
      probe.ref ! new String(writeMessage.bytes.toByteBuffer.array())
      writeMessage
    }
  }

  override def beforeAll(): Unit = {
    startRabbit()
  }

  override def afterAll(): Unit = {
    stopRabbit()
  }

  "RabbitEventSink" should {
    val sink: RabbitEventSink[String] = new RabbitEventSink[String] {
      override def rabbitMqUri: String = "localhost:5672"
      override def queueName: String = "queue"

      override def autoDeclarePlan: Option[AutoDeclarePlan] = None
      override def writeRoute: String = "route"
      override def formatting: SurgeEventWriteFormatting[String] = mock(classOf[SurgeEventWriteFormatting[String]])
    }

    "Have default configs" in {
      sink.autoDeclarePlan shouldEqual None
      sink.queueName shouldEqual "queue"
      sink.writeRoute shouldEqual "route"
      sink.rabbitMqUri shouldEqual "localhost:5672"
    }

    "Apply declarations when autoDeclare plan is set" in {
      val uriInfo = rabbitMqUri()
      val testProbe = TestProbe()

      // Create Sink
      val sink: RabbitEventSink[String] = spy(new TestRabbitEventSink(
        uriInfo.uri,
        probe = testProbe,
        queueName = "apply-declarations-queue-name", writeRoute = "route", autoDeclarePlan = Some(AutoDeclarePlan(
          QueuePlan("apply-declarations-queue-name", durable = false, autoDelete = false, exclusive = false),
          ExchangePlan("apply-declarations-exchange", durable = false, autoDelete = false),
          Binding("apply-declarations-queue-name", "apply-declarations-exchange", Some("route"))))))

      // Read from queue
      val declarations: Vector[Declaration] = Seq(
        sink.autoDeclarePlan.get.queuePlan.declaration(),
        sink.autoDeclarePlan.get.exchangePlan.declaration(),
        sink.autoDeclarePlan.get.binding.declaration()).toVector
      AmqpSource.committableSource(
        NamedQueueSourceSettings(AmqpUriConnectionProvider(uriInfo.uri), "apply-declarations-queue-name")
          .withDeclarations(declarations), bufferSize = 10)
        .runWith(Sink.ignore)

      // Write to sink
      Source.single("test message").map(m => EventPlusStreamMeta[String, String, NotUsed](
        messageKey = m, m, NotUsed.notUsed(), Map.empty)).viaMat(sink.eventHandler[NotUsed])(Keep.both).to(Sink.ignore)
        .run()

      // Check if declarations applied
      val declarationsCaptor: ArgumentCaptor[Seq[Declaration]] = ArgumentCaptor.forClass(classOf[Seq[Declaration]])
      verify(sink, times(1)).logDeclarations(declarationsCaptor.capture())
      declarationsCaptor.getValue.size shouldEqual 3
      declarationsCaptor.getValue.exists(d => d.isInstanceOf[QueueDeclaration]) shouldEqual true
      declarationsCaptor.getValue.exists(d => d.isInstanceOf[ExchangeDeclaration]) shouldEqual true
      declarationsCaptor.getValue.exists(d => d.isInstanceOf[BindingDeclaration]) shouldEqual true

      // Check Queue Declaration matches QueuePlan
      declarationsCaptor.getValue.find(d => d.isInstanceOf[QueueDeclaration])
        .get.asInstanceOf[QueueDeclaration].name shouldEqual "apply-declarations-queue-name"
      declarationsCaptor.getValue.find(d => d.isInstanceOf[QueueDeclaration])
        .get.asInstanceOf[QueueDeclaration].exclusive shouldEqual false
      declarationsCaptor.getValue.find(d => d.isInstanceOf[QueueDeclaration])
        .get.asInstanceOf[QueueDeclaration].durable shouldEqual false
      declarationsCaptor.getValue.find(d => d.isInstanceOf[QueueDeclaration])
        .get.asInstanceOf[QueueDeclaration].autoDelete shouldEqual false

      // Check Exchange Declaration matches ExchangePlan
      declarationsCaptor.getValue.find(d => d.isInstanceOf[ExchangeDeclaration])
        .get.asInstanceOf[ExchangeDeclaration].name shouldEqual "apply-declarations-exchange"
      declarationsCaptor.getValue.find(d => d.isInstanceOf[ExchangeDeclaration])
        .get.asInstanceOf[ExchangeDeclaration].durable shouldEqual false
      declarationsCaptor.getValue.find(d => d.isInstanceOf[ExchangeDeclaration])
        .get.asInstanceOf[ExchangeDeclaration].autoDelete shouldEqual false

      // Check Binding Declaration matches Binding
      declarationsCaptor.getValue.find(d => d.isInstanceOf[BindingDeclaration])
        .get.asInstanceOf[BindingDeclaration].queue shouldEqual "apply-declarations-queue-name"
      declarationsCaptor.getValue.find(d => d.isInstanceOf[BindingDeclaration])
        .get.asInstanceOf[BindingDeclaration].exchange shouldEqual "apply-declarations-exchange"
      declarationsCaptor.getValue.find(d => d.isInstanceOf[BindingDeclaration])
        .get.asInstanceOf[BindingDeclaration].routingKey shouldEqual Some("route")

      verify(sink, times(1)).declarations(ArgumentMatchers.any(classOf[AutoDeclarePlan]))
    }

    "Should publish when publisher does autoDeclare" in {
      val uriInfo = rabbitMqUri()
      val testProbe = TestProbe()

      // Create Sink
      val sink: RabbitEventSink[String] = spy(new TestRabbitEventSink(
        uriInfo.uri,
        probe = testProbe,
        queueName = "publish-auto-declare-queue-name", writeRoute = "route", autoDeclarePlan = Some(AutoDeclarePlan(
          QueuePlan("publish-auto-declare-queue-name", durable = false, autoDelete = false, exclusive = false),
          ExchangePlan("publish-auto-declare-exchange", durable = false, autoDelete = false),
          Binding("publish-auto-declare-queue-name", "publish-auto-declare-exchange", Some("route"))))))

      // Read from queue and expect message
      AmqpSource.committableSource(
        NamedQueueSourceSettings(AmqpUriConnectionProvider(uriInfo.uri), "publish-auto-declare-queue-name")
          .withDeclarations(sink.autoDeclarePlan.get.declarations().toVector), bufferSize = 10)
        .runWith(Sink.ignore)

      // Write to sink
      Source.single(element = "test message").map(m => EventPlusStreamMeta[String, String, NotUsed](
        messageKey = m, m, NotUsed.notUsed(), Map.empty)).viaMat(sink.eventHandler[NotUsed])(Keep.both).to(Sink.ignore)
        .run()

      testProbe.expectMsg(10.seconds, obj = "test message")

      val writeMessageCaptor: ArgumentCaptor[WriteMessage] = ArgumentCaptor.forClass(classOf[WriteMessage])
      verify(sink, times(1)).interceptWriteMessage(writeMessageCaptor.capture())

      val capturedWriteMessage: WriteMessage = writeMessageCaptor.getValue
      capturedWriteMessage.routingKey shouldEqual Some("route")
      capturedWriteMessage.bytes shouldEqual "test message".getBytes()
    }
  }
}