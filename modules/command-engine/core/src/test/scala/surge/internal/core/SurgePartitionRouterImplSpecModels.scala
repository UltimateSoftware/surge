// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.core

import akka.actor.{ ActorContext, Props }
import akka.testkit.TestProbe
import org.apache.kafka.common.TopicPartition
import surge.akka.cluster.{ EntityPropsProvider, PerShardLogicProvider }
import surge.core.TestBoundedContext.{ BaseTestCommand, WrappedTestCommand }
import surge.core.{ Controllable, ControllableAdapter, TestBoundedContext }
import surge.internal.akka.ActorWithTracing
import surge.internal.tracing.NoopTracerFactory
import surge.kafka.PersistentActorRegionCreator
import surge.kafka.streams.{ HealthCheck, HealthCheckStatus }

import scala.concurrent.Future

object SurgePartitionRouterImplSpecModels extends TestBoundedContext {

  class ProbeInterceptorActor(topicPartition: TopicPartition, probe: TestProbe) extends ActorWithTracing {
    implicit val tracer = NoopTracerFactory.create()
    override def receive: Receive = { case cmd: BaseTestCommand =>
      probe.ref.forward(WrappedTestCommand(topicPartition, cmd))
    }
  }

  class ProbeInterceptorRegionCreator(probe: TestProbe) extends PersistentActorRegionCreator[String] {
    override def regionFromTopicPartition(topicPartition: TopicPartition): PerShardLogicProvider[String] = {
      val provider = new PerShardLogicProvider[String] {
        override def actorProvider(context: ActorContext): EntityPropsProvider[String] = (_: String) => Props(new ProbeInterceptorActor(topicPartition, probe))
        override def onShardTerminated(): Unit = {}
        override def healthCheck(): Future[HealthCheck] = Future.successful(HealthCheck("test", "test", HealthCheckStatus.UP))

        override val controllable: Controllable = new ControllableAdapter
      }

      provider.controllable.start()
      provider
    }
  }
}
