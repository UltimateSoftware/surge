// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.config

import java.util.concurrent.TimeUnit

import com.typesafe.config.ConfigFactory

import scala.concurrent.duration._

object TimeoutConfig {
  private val config = ConfigFactory.load()

  object StateStoreKafkaStreamActor {
    val askTimeout: FiniteDuration =
      config.getDuration("surge.state-store-actor.ask-timeout", TimeUnit.MILLISECONDS).milliseconds
  }

  object AggregateActor {
    val idleTimeout: FiniteDuration =
      config.getDuration("surge.aggregate-actor.idle-timeout", TimeUnit.MILLISECONDS).milliseconds
    val askTimeout: FiniteDuration =
      config.getDuration("surge.aggregate-actor.ask-timeout", TimeUnit.MILLISECONDS).milliseconds
    val initTimeout: FiniteDuration =
      config.getDuration("surge.aggregate-actor.init-timeout", TimeUnit.MILLISECONDS).milliseconds
  }

  object HealthCheck {
    val actorAskTimeout: FiniteDuration = 10.seconds
  }

  object PartitionTracker {
    val updateTimeout: FiniteDuration = 20.seconds
  }

  object ActorRegistry {
    val askTimeout: FiniteDuration = config.getDuration("surge.actor-registry.ask-timeout", TimeUnit.MILLISECONDS).milliseconds
    val resolveActorTimeout: FiniteDuration = config.getDuration("surge.actor-registry.resolve-actor-timeout", TimeUnit.MILLISECONDS).milliseconds
  }

  object PublisherActor {
    val publishTimeout: FiniteDuration =
      config.getDuration("surge.producer.publish-timeout", TimeUnit.MILLISECONDS).milliseconds
    val aggregateStateCurrentTimeout: FiniteDuration =
      config.getDuration("surge.producer.aggregate-state-current-timeout", TimeUnit.MILLISECONDS).milliseconds
  }

}
