// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.rabbit

import akka.actor.ActorSystem
import akka.stream.ClosedShape
import akka.stream.alpakka.amqp._
import akka.stream.alpakka.amqp.scaladsl.{ AmqpSink, CommittableReadResult }
import akka.stream.scaladsl.{ GraphDSL, RunnableGraph, Sink, Source }
import akka.testkit.{ TestKit, TestProbe }
import akka.util.ByteString
import akka.{ Done, NotUsed }
import org.mockito.{ ArgumentCaptor, ArgumentMatchers, Mockito }
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import surge.core.DataPipeline.ReplaySuccessfullyStarted
import surge.core.{ DataPipeline, EventSink, SurgeEventReadFormatting }

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{ Await, Future }

object RabbitEventSourceSpec {
  val SERVICE_PORT: Int = 5672
}

class RabbitEventSourceSpec extends TestKit(ActorSystem("RabbitEventSourceSpec"))
  with EmbeddedRabbit with Matchers with AnyWordSpecLike with BeforeAndAfterAll {
  import Mockito._

  class TestRabbitEventSource(val rabbitMqUri: String, val queueName: String,
      override val bufferSize: Int = 10,
      override val autoDeclarePlan: Option[AutoDeclarePlan]) extends RabbitEventSource[String] {
    private var sourcedReadResults: mutable.Seq[CommittableReadResult] = mutable.Seq[CommittableReadResult]()

    override def actorSystem: ActorSystem = system
    override def formatting: SurgeEventReadFormatting[String] = (bytes: Array[Byte]) => new String(bytes)

    override protected[rabbit] def interceptReadResult(result: CommittableReadResult): CommittableReadResult = {
      sourcedReadResults = sourcedReadResults ++ Seq(result)
      result
    }

    def recorded(): Seq[CommittableReadResult] = sourcedReadResults
  }

  class TestProbeSink(probe: TestProbe) extends EventSink[String] {
    override def parallelism: Int = 16
    override def handleEvent(key: String, event: String, headers: Map[String, Array[Byte]]): Future[Any] = {
      probe.ref ! event
      Future.successful(Done)
    }
    override def partitionBy(key: String, event: String, headers: Map[String, Array[Byte]]): String = event
  }

  override def beforeAll(): Unit = {
    startRabbit()
  }

  override def afterAll(): Unit = {
    stopRabbit()
  }

  "RabbitEventSource" should {
    "Have default configs" in {
      val uriInfo = rabbitMqUri()
      val source: TestRabbitEventSource = new TestRabbitEventSource(
        rabbitMqUri = uriInfo.uri,
        queueName = "temp-test-queue-name",
        autoDeclarePlan = None)

      source.autoDeclarePlan.isDefined shouldEqual false

      source.queueName shouldEqual "temp-test-queue-name"
      source.rabbitMqUri shouldEqual uriInfo.uri
    }

    "Apply declarations when autoDeclare plan is set" in {
      val uriInfo = rabbitMqUri()
      val rabbitSource: TestRabbitEventSource = spy(new TestRabbitEventSource(
        rabbitMqUri = uriInfo.uri,
        queueName = "temp-test-queue-name",
        autoDeclarePlan = Some(AutoDeclarePlan(
          QueuePlan("temp-test-queue-name", exclusive = false, durable = false, autoDelete = false),
          ExchangePlan("exchange", durable = false, autoDelete = false),
          Binding("temp-test-queue-name", "exchange", Some("route"))))))
      val testProbe = TestProbe()

      val pipeline = rabbitSource.to(new TestProbeSink(testProbe))
      pipeline.start()

      // Check if declarations applied
      val declarationsCaptor: ArgumentCaptor[Seq[Declaration]] = ArgumentCaptor.forClass(classOf[Seq[Declaration]])
      verify(rabbitSource, times(1)).logDeclarations(declarationsCaptor.capture())
      declarationsCaptor.getValue.size shouldEqual 3
      declarationsCaptor.getValue.exists(d => d.isInstanceOf[QueueDeclaration]) shouldEqual true
      declarationsCaptor.getValue.exists(d => d.isInstanceOf[ExchangeDeclaration]) shouldEqual true
      declarationsCaptor.getValue.exists(d => d.isInstanceOf[BindingDeclaration]) shouldEqual true

      // Check Queue Declaration matches QueuePlan
      declarationsCaptor.getValue.find(d => d.isInstanceOf[QueueDeclaration]).get.asInstanceOf[QueueDeclaration].name shouldEqual "temp-test-queue-name"
      declarationsCaptor.getValue.find(d => d.isInstanceOf[QueueDeclaration]).get.asInstanceOf[QueueDeclaration].exclusive shouldEqual false
      declarationsCaptor.getValue.find(d => d.isInstanceOf[QueueDeclaration]).get.asInstanceOf[QueueDeclaration].durable shouldEqual false
      declarationsCaptor.getValue.find(d => d.isInstanceOf[QueueDeclaration]).get.asInstanceOf[QueueDeclaration].autoDelete shouldEqual false

      // Check Exchange Declaration matches ExchangePlan
      declarationsCaptor.getValue.find(d => d.isInstanceOf[ExchangeDeclaration]).get.asInstanceOf[ExchangeDeclaration].name shouldEqual "exchange"
      declarationsCaptor.getValue.find(d => d.isInstanceOf[ExchangeDeclaration]).get.asInstanceOf[ExchangeDeclaration].durable shouldEqual false
      declarationsCaptor.getValue.find(d => d.isInstanceOf[ExchangeDeclaration]).get.asInstanceOf[ExchangeDeclaration].autoDelete shouldEqual false

      // Check Binding Declaration matches Binding
      declarationsCaptor.getValue.find(d => d.isInstanceOf[BindingDeclaration]).get.asInstanceOf[BindingDeclaration].queue shouldEqual "temp-test-queue-name"
      declarationsCaptor.getValue.find(d => d.isInstanceOf[BindingDeclaration]).get.asInstanceOf[BindingDeclaration].exchange shouldEqual "exchange"
      declarationsCaptor.getValue.find(d => d.isInstanceOf[BindingDeclaration]).get.asInstanceOf[BindingDeclaration].routingKey shouldEqual Some("route")

      verify(rabbitSource, times(1)).declarations(ArgumentMatchers.any(classOf[AutoDeclarePlan]))

      rabbitSource.autoDeclarePlan.isDefined shouldEqual true

      pipeline.stop()
    }

    "Not apply declarations when autoDeclare plan is not set" in {
      val uriInfo = rabbitMqUri()
      val rabbitSource: TestRabbitEventSource = spy(new TestRabbitEventSource(
        rabbitMqUri = uriInfo.uri,
        queueName = "temp-test-queue-name",
        autoDeclarePlan = None))
      val testProbe = TestProbe()

      val pipeline = rabbitSource.to(new TestProbeSink(testProbe))
      pipeline.start()

      verify(rabbitSource, never()).declarations(ArgumentMatchers.any(classOf[AutoDeclarePlan]))

      pipeline.stop()
    }

    "Subscribe to rabbit when subscriber does not autoDeclare" in {
      val messageRoute = "route"
      val uriInfo = rabbitMqUri()

      // Publisher declares queue/exchange/binding
      // Publish to rabbit "test message 1"
      val futureDone: Future[Done] = primeQueue(Source.single(ByteString("test message 1")), amqpSink(
        host = uriInfo.host,
        port = uriInfo.port,
        queueName = "subscribe-no-auto-declare-queue-name",
        exchangeName = "subscribe-no-auto-declare-exchange",
        writeRoute = messageRoute))

      // Subscribe
      val testProbe = TestProbe()
      val rabbitSource: TestRabbitEventSource = new TestRabbitEventSource(
        rabbitMqUri = uriInfo.uri,
        queueName = "subscribe-no-auto-declare-queue-name",
        autoDeclarePlan = None)

      val pipeline = rabbitSource.to(new TestProbeSink(testProbe))

      // Start
      pipeline.start()

      // Wait for write to complete
      Await.result(futureDone, 10.seconds)

      // Expect message received
      testProbe.expectMsg(max = 10.seconds, "test message 1")
      rabbitSource.recorded().size shouldEqual 1
      // Stop
      pipeline.stop()
    }

    "Subscribe to rabbit when subscriber does autoDeclare" in {
      val messageRoute = "route"
      val uriInfo = rabbitMqUri()
      // Subscribe
      val testProbe = TestProbe()
      val rabbitSource = new TestRabbitEventSource(
        rabbitMqUri = uriInfo.uri,
        queueName = "subscribe-auto-declare-queue-name",
        autoDeclarePlan = Some(AutoDeclarePlan(
          queuePlan = QueuePlan("subscribe-auto-declare-queue-name", exclusive = false, durable = false, autoDelete = false),
          exchangePlan = ExchangePlan("subscribe-auto-declare-exchange", durable = false, autoDelete = false),
          binding = Binding("subscribe-auto-declare-queue-name", "subscribe-auto-declare-exchange", Some(messageRoute)))))

      val pipeline = rabbitSource.to(new TestProbeSink(testProbe))

      // Start
      pipeline.start()

      // Publish to rabbit "test message 1"
      val futureDone: Future[Done] = primeQueue(Source.single(ByteString("test message 1")), amqpSink(
        host = uriInfo.host,
        port = uriInfo.port,
        queueName = "subscribe-auto-declare-queue-name",
        exchangeName = "subscribe-auto-declare-exchange",
        writeRoute = messageRoute))

      // Wait for write to complete
      Await.result(futureDone, 10.seconds)

      // Expect message received
      testProbe.expectMsg(max = 10.seconds, "test message 1")
      rabbitSource.recorded().size shouldEqual 1

      // Stop
      pipeline.stop()
    }

    "Replay" in {
      val messageRoute = "route"
      val uriInfo = rabbitMqUri()
      // Subscribe
      val testProbe = TestProbe()
      val rabbitSource = new TestRabbitEventSource(
        rabbitMqUri = uriInfo.uri,
        queueName = "subscribe-auto-declare-queue-name",
        autoDeclarePlan = Some(AutoDeclarePlan(
          queuePlan = QueuePlan("subscribe-auto-declare-queue-name", exclusive = false, durable = false, autoDelete = false),
          exchangePlan = ExchangePlan("subscribe-auto-declare-exchange", durable = false, autoDelete = false),
          binding = Binding("subscribe-auto-declare-queue-name", "subscribe-auto-declare-exchange", Some(messageRoute)))))

      val pipeline = rabbitSource.to(new TestProbeSink(testProbe))

      val result: DataPipeline.ReplayResult = Await.result(pipeline.replay(), timeout())

      result shouldBe a[ReplaySuccessfullyStarted]
    }
  }

  private def amqpSink(
    host: String,
    port: Int,
    queueName: String,
    exchangeName: String,
    writeRoute: String): Sink[ByteString, Future[Done]] = {
    val queueDeclaration: Declaration = QueueDeclaration(queueName)
      .withDurable(durable = false)
      .withAutoDelete(autoDelete = false)
    val exchangeDeclaration: Declaration = ExchangeDeclaration(exchangeName, "topic")
      .withDurable(durable = false)
      .withAutoDelete(autoDelete = false)
    val bindingDeclaration: Declaration = BindingDeclaration(queueName, exchangeName)
      .withRoutingKey(writeRoute)
    val settings = AmqpWriteSettings(AmqpUriConnectionProvider(s"amqp://tester:tester@$host:$port/vhost"))
      .withDeclarations(Seq(queueDeclaration, exchangeDeclaration, bindingDeclaration).toList.asJava)
      .withRoutingKey(writeRoute)
      .withExchange(exchangeName)
    AmqpSink.simple(settings)
  }

  private def primeQueue(source: Source[ByteString, NotUsed], amqpSink: Sink[ByteString, Future[Done]]): Future[Done] = {
    val graph = RunnableGraph.fromGraph(GraphDSL.create(amqpSink) { implicit builder => s =>
      import GraphDSL.Implicits._
      source ~> {
        s.in
      }
      ClosedShape
    })
    graph.run()
  }
}
