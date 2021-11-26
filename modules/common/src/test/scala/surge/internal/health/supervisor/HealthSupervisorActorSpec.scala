// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.health.supervisor

import akka.actor.{ActorRef, ActorSystem}
import akka.pattern.ask
import akka.testkit.{TestKit, TestProbe}
import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Milliseconds, Seconds, Span}
import org.scalatest.wordspec.AnyWordSpecLike
import surge.core.{Ack, ControllableAdapter}
import surge.health._
import surge.health.config.{HealthSupervisorConfig, ThrottleConfig, WindowingStreamConfig, WindowingStreamSliderConfig}
import surge.health.domain.{HealthSignal, Trace}
import surge.health.jmx.Domain.HealthRegistrationDetail
import surge.health.jmx.View.HealthRegistrationDetailMxView
import surge.health.matchers.{SideEffect, SignalPatternMatcherDefinition}
import surge.health.supervisor.Api.{QueryComponentExists, RestartComponent, ShutdownComponent}
import surge.internal.health._
import surge.internal.health.windows.stream.sliding.SlidingHealthSignalStreamProvider

import java.lang.management.ManagementFactory
import java.util.regex.Pattern
import javax.management.openmbean.CompositeData
import javax.management.{MBeanInfo, MBeanServer, ObjectName}
import scala.concurrent.Future
import scala.concurrent.duration._

class HealthSupervisorActorSpec
    extends TestKit(ActorSystem("HealthSignalSupervisorSpec", ConfigFactory.load("health-supervisor-actor-spec")))
    with AnyWordSpecLike
    with ScalaFutures
    with BeforeAndAfterAll
    with Matchers
    with Eventually {
  implicit override val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = scaled(Span(30, Seconds)), interval = scaled(Span(10, Milliseconds)))

  private val testHealthSignal = HealthSignal(topic = "health.signal", name = "boom", signalType = SignalType.TRACE, data = Trace("test"), source = None)

  case class TestContext(probe: TestProbe, bus: HealthSignalBusTrait, mbs: MBeanServer, objectName: ObjectName, beanInfo: MBeanInfo)

  def testContext[T](testFun: TestContext => T): T = {
    import scala.jdk.CollectionConverters._
    val healthSupervisorConfig =
      HealthSupervisorConfig(jmxEnabled = ConfigFactory.load("health-supervisor-actor-spec").getBoolean("surge.health.supervisor-actor.jmx-enabled"))
    val probe = TestProbe()
    val bus = new SlidingHealthSignalStreamProvider(
      WindowingStreamConfig(
        advancerConfig = WindowingStreamSliderConfig(buffer = 10, advanceAmount = 1),
        throttleConfig = ThrottleConfig(elements = 100, duration = 5.seconds),
        windowingInitDelay = 10.milliseconds,
        windowingResumeDelay = 10.milliseconds,
        maxWindowSize = 500),
      system,
      streamMonitoring = Some(new StreamMonitoringRef(probe.ref)),
      healthSupervisionConfig = healthSupervisorConfig,
      patternMatchers = Seq(
        SignalPatternMatcherDefinition
          .nameEquals(signalName = "test.trace", frequency = 100.milliseconds, sideEffect = Some(SideEffect(Seq(testHealthSignal)))))).bus()
    val mbs = ManagementFactory.getPlatformMBeanServer
    val found = eventually {
      val objectNames: java.util.Set[ObjectName] = mbs.queryNames(null, null)
      val found = objectNames.asScala.find(name => name.getCanonicalName.contains("SurgeHealth"))
      found.nonEmpty shouldEqual true

      found
    }

    found shouldBe defined

    // Health Registrations should be empty
    val beanInfo = mbs.getMBeanInfo(found.get)
    val objectName = found.get
    try {
      testFun(TestContext(probe, bus, objectName = objectName, beanInfo = beanInfo, mbs = mbs))
    } finally {
      bus.unsupervise().signalStream().stop()
    }
  }

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system, verifySystemShutdown = true)
  }

  "HealthSupervisorActorSpec" should {
    // Ignore: not working in pipeline
    "expose jmx management bean" ignore testContext { ctx =>
      import scala.jdk.CollectionConverters._

      val beanInfo: MBeanInfo = ctx.beanInfo
      val jmxBeanName = ctx.objectName

      val attributeValues = ctx.mbs.getAttributes(jmxBeanName, beanInfo.getAttributes.map(info => info.getName))
      val maybeHealthRegistry = attributeValues.asList().asScala.find(att => att.getName == "HealthRegistry")

      maybeHealthRegistry shouldBe defined

      // Health Registrations should be empty
      maybeHealthRegistry.get.getValue shouldBe a[CompositeData]
      maybeHealthRegistry.get.getValue.asInstanceOf[CompositeData].get("registrations").asInstanceOf[Array[CompositeData]].isEmpty shouldEqual true

      val operations = beanInfo.getOperations
      operations.toSeq.nonEmpty shouldEqual true
    }

    // Ignore: not working in pipeline
    "stop component using jmx bean" ignore testContext { ctx =>
      import ctx._

      import scala.jdk.CollectionConverters._

      val componentName = "testRegistrationControl"
      val stopProbe = TestProbe()
      val control = new ControllableAdapter() {
        override def stop(): Future[Ack] = Future {
          stopProbe.ref ! ShutdownComponent(componentName, probe.ref)
          Ack()
        }(system.dispatcher)
      }

      // Register
      bus.register(control, componentName = componentName, Seq()).futureValue shouldBe an[Ack]

      val registryList = eventually {
        val jmxBeanAttributes = ctx.mbs.getAttributes(ctx.objectName, ctx.beanInfo.getAttributes.map(att => att.getName))
        val maybeHealthRegistry = jmxBeanAttributes.asList().asScala.find(att => att.getName == "HealthRegistry")

        maybeHealthRegistry shouldBe defined
        maybeHealthRegistry.get.getValue shouldBe a[CompositeData]

        val registryAsList = maybeHealthRegistry.get.getValue.asInstanceOf[CompositeData].get("registrations").asInstanceOf[Array[CompositeData]]
        registryAsList.exists(detail => detail.get("componentName") == componentName) shouldEqual true

        registryAsList.toSeq
          .map(d => HealthRegistrationDetailMxView(HealthRegistrationDetail(d.get("componentName").toString, d.get("controlRefPath").toString)))
          .toList
      }

      registryList.size shouldEqual 1

      // Attempt to stop
      val maybeStopOperation = beanInfo.getOperations.toList.find(op => op.getName == "stop")
      maybeStopOperation shouldBe defined

      val actorSelection = system.actorSelection(registryList.head.sender)

      val controlRef: ActorRef = actorSelection.resolveOne(10.seconds).futureValue
      controlRef.ask(QueryComponentExists(componentName))(30.seconds).futureValue shouldEqual true

      mbs.invoke(objectName, maybeStopOperation.get.getName, Array(componentName), Array("java.lang.String"))

      // Verify stopped
      eventually {
        val jmxBeanAttributes = ctx.mbs.getAttributes(ctx.objectName, ctx.beanInfo.getAttributes.map(att => att.getName))
        val maybeHealthRegistry = jmxBeanAttributes.asList().asScala.find(att => att.getName == "HealthRegistry")

        maybeHealthRegistry shouldBe defined
        maybeHealthRegistry.get.getValue shouldBe a[CompositeData]

        val registryAsList = maybeHealthRegistry.get.getValue.asInstanceOf[CompositeData].get("registrations").asInstanceOf[Array[CompositeData]].toList
        registryAsList.nonEmpty shouldEqual false

        // Component should not exist in registry after stopped.
        eventually {
          actorSelection.resolveOne(10.seconds).futureValue.ask(QueryComponentExists(componentName))(30.seconds).futureValue shouldEqual false
        }
      }
    }

    "sliding stream; attempt to restart registered actor" in testContext { ctx =>
      import ctx._
      // Start signal streaming
      bus.signalStream().start()

      val componentName = "boomControl"
      val restartProbe = TestProbe()
      val control = new ControllableAdapter() {
        override def restart(): Future[Ack] = Future {
          restartProbe.ref ! RestartComponent(componentName, probe.ref)
          Ack()
        }(system.dispatcher)
      }

      // Register
      bus.register(control, componentName = componentName, Seq(Pattern.compile("boom"))).futureValue shouldBe an[Ack]

      // Signal
      bus.signalWithTrace(name = "test.trace", Trace("test trace")).emit()

      // Verify restart
      val restart = restartProbe.expectMsgType[RestartComponent]
      restart.name shouldEqual componentName
    }

    "receive registration" in testContext { ctx =>
      import ctx._
      val ref: HealthSupervisorTrait = bus.supervisor().get

      val control = new ControllableAdapter()
      val message = bus.registration(control, componentName = "boomControl", Seq.empty)

      ref.register(message.underlyingRegistration()).futureValue shouldBe an[Ack]

      // FIXME expectMsgType below has the same problem as described below where it receives a bunch of
      //  window updates in addition to the HealthRegistrationReceived
      eventually {
        val received = probe.expectMsgType[HealthRegistrationReceived]
        received.registration.componentName shouldEqual message.underlyingRegistration().componentName
      }
      ref.registrationLinks().exists(l => l.componentName == "boomControl")
      ref.stop()
    }

    "unregister control when it stops" in testContext { ctx =>
      import ctx._
      val ref: HealthSupervisorTrait = bus.supervisor().get

      val control = new ControllableAdapter()
      val message = bus.registration(control, componentName = "boomControl", Seq.empty)

      ref.register(message.underlyingRegistration()).futureValue shouldBe an[Ack]

      ref.registrationLinks().exists(l => l.componentName == "boomControl") shouldEqual true

      ref
        .registrationLinks()
        .find(l => l.componentName == "boomControl")
        .foreach(link => {
          link.controlProxy.shutdown(probe.ref)
        })

      eventually {
        ref.registrationLinks().exists(l => l.componentName == "boomControl") shouldEqual false
      }
    }

    "receive signal" in testContext { ctx =>
      import ctx._
      val ref: HealthSupervisorTrait = bus.supervisor().get

      // FIXME now that things start up so quickly in this test the message below can be emitted to the bus before anything
      //  is listening to it. It will require a bit of plumbing to check that it's started before sending the message so
      //  going with the quick and dirty Thread.sleep for now. Rather than plumbing that all the way through, maybe this test wants to be
      //  refactored a bit to focus on just the HealthSupervisorActor aspect of things
      Thread.sleep(1000L)

      val message = bus.signalWithTrace(name = "test", Trace("test trace"))
      message.emit()

      // FIXME This probe is receiving a ton of messages. Ideally it should register itself
      //  in a way that it would just receive these relevant HealthSignalReceived messages and
      //  not additionally receive all of the windowing events.
      val received = probe.fishForMessage(max = 3.seconds) { case msg =>
        msg.isInstanceOf[HealthSignalReceived]
      }

      received.asInstanceOf[HealthSignalReceived].signal shouldEqual message.underlyingSignal()

      ref.stop()
    }
  }

  "HealthSignalStreamMonitoringRefWithSupervisionSupport" should {
    import org.mockito.Mockito._
    "proxy to actorRef" in {
      val probe = TestProbe()
      val monitor = new HealthSignalStreamMonitoringRefWithSupervisionSupport(actor = probe.ref)

      monitor.registrationReceived(mock(classOf[HealthRegistrationReceived]))
      monitor.healthSignalReceived(mock(classOf[HealthSignalReceived]))

      probe.expectMsgClass(classOf[HealthRegistrationReceived])
      probe.expectMsgClass(classOf[HealthSignalReceived])
    }
  }
}
