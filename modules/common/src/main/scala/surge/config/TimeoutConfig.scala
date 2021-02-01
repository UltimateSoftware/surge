// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.config

import java.util.concurrent.TimeUnit

import com.typesafe.config.ConfigFactory

import scala.concurrent.duration._

object TimeoutConfig {
  private val config = ConfigFactory.load()

  val debugTimeoutEnabled: Boolean = config.getBoolean("surge.debug-mode-timeouts-enabled")

  private val timeoutScaleFactor = if (debugTimeoutEnabled) {
    1000
  } else {
    1
  }

  object StateStoreKafkaStreamActor {
    val askTimeout: FiniteDuration =
      config.getDuration("surge.state-store-actor.ask-timeout", TimeUnit.MILLISECONDS).milliseconds * timeoutScaleFactor
  }

  object AggregateActor {
    val idleTimeout: FiniteDuration =
      config.getDuration("surge.aggregate-actor.idle-timeout", TimeUnit.MILLISECONDS).milliseconds * timeoutScaleFactor
    val askTimeout: FiniteDuration =
      config.getDuration("surge.aggregate-actor.ask-timeout", TimeUnit.MILLISECONDS).milliseconds * timeoutScaleFactor
  }

  object HealthCheck {
    val actorAskTimeout: FiniteDuration = 10.seconds * timeoutScaleFactor
  }

  object Kafka {
    val consumerSessionTimeout: FiniteDuration = if (debugTimeoutEnabled) {
      // Set this one explicitly instead of leveraging the scale factor because we can't exceed the broker configured group max timeout
      10.minutes
    } else {
      // TODO is there a good way to get this default from the Kafka client libraries?
      10.seconds
    }
  }

  object PartitionTracker {
    val updateTimeout: FiniteDuration = 20.seconds * timeoutScaleFactor
  }

  object ActorRegistry {
    val askTimeout = 3.seconds * timeoutScaleFactor
    val resolveActorTimeout = 3.seconds * timeoutScaleFactor
  }

  object PublisherActor {
    val publishTimeout: FiniteDuration =
      config.getDuration("surge.producer.publish-timeout", TimeUnit.MILLISECONDS).milliseconds * timeoutScaleFactor
    val aggregateStateCurrentTimeout: FiniteDuration = 10.seconds * timeoutScaleFactor
  }

}