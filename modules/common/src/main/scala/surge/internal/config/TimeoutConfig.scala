// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.internal.config

import java.util.concurrent.TimeUnit

import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.consumer.ConsumerConfig

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
  }

  object HealthCheck {
    val actorAskTimeout: FiniteDuration = 10.seconds
  }

  object Kafka {
    // TODO is there a good way to get this default from the Kafka client libraries?
    val consumerSessionTimeout: FiniteDuration = 10.seconds
  }

  object PartitionTracker {
    val updateTimeout: FiniteDuration = 20.seconds
  }

  object ActorRegistry {
    val askTimeout: FiniteDuration = 3.seconds
    val resolveActorTimeout: FiniteDuration = 3.seconds
  }

  object PublisherActor {
    val publishTimeout: FiniteDuration =
      config.getDuration("surge.producer.publish-timeout", TimeUnit.MILLISECONDS).milliseconds
    val aggregateStateCurrentTimeout: FiniteDuration = 10.seconds
  }

}
