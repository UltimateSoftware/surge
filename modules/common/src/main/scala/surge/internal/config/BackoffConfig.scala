// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.config

import java.time.Duration

import com.typesafe.config.ConfigFactory

object BackoffConfig {
  private val config = ConfigFactory.load()

  object StateStoreKafkaStreamActor {
    val minBackoff = config.getDuration("surge.state-store-actor.backoff.min-backoff")
    val maxBackoff = config.getDuration("surge.state-store-actor.backoff.max-backoff")
    val randomFactor = config.getDouble("surge.state-store-actor.backoff.random-factor")
    val maxRetries = config.getInt("surge.state-store-actor.backoff.max-retries")
  }

  object GlobalKTableKafkaStreamActor {
    val minBackoff = config.getDuration("surge.global-ktable-actor.backoff.min-backoff")
    val maxBackoff = config.getDuration("surge.global-ktable-actor.backoff.max-backoff")
    val randomFactor = config.getDouble("surge.global-ktable-actor.backoff.random-factor")
    val maxRetries = config.getInt("surge.global-ktable-actor.backoff.max-retries")
  }

  object HealthSignalWindowActor {
    val minBackoff = config.getDuration("surge.health.window-actor.backoff.min-backoff")
    val maxBackoff = config.getDuration("surge.health.window-actor.backoff.max-backoff")
    val randomFactor = config.getDouble("surge.health.window-actor.backoff.random-factor")
    val maxRetries = config.getInt("surge.health.window-actor.backoff.max-retries")
  }

  object HealthSupervisorActor {
    val minBackoff: Duration = config.getDuration("surge.health.supervisor-actor.backoff.min-backoff")
    val maxBackoff: Duration = config.getDuration("surge.health.supervisor-actor.backoff.max-backoff")
    val randomFactor: Double = config.getDouble("surge.health.supervisor-actor.backoff.random-factor")
    val maxRetries: Int = config.getInt("surge.health.supervisor-actor.backoff.max-retries")
  }
}
