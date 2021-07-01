// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.config

import com.typesafe.config.ConfigFactory

object BackoffConfig {
  private val config = ConfigFactory.load()

  object StateStoreKafkaStreamActor {
    val minBackoff = config.getDuration("surge.state-store-actor.backoff.min-backoff")
    val maxBackoff = config.getDuration("surge.state-store-actor.backoff.max-backoff")
    val randomFactor = config.getDouble("surge.state-store-actor.backoff.random-factor")
    val maxRetries = config.getInt("surge.state-store-actor.backoff.max-retries")
  }

  object HealthSignalWindowActor {
    val minBackoff = config.getDuration("surge.health.window-actor.backoff.min-backoff")
    val maxBackoff = config.getDuration("surge.health.window-actor.backoff.max-backoff")
    val randomFactor = config.getDouble("surge.health.window-actor.backoff.random-factor")
    val maxRetries = config.getInt("surge.health.window-actor.backoff.max-retries")
  }

  object HealthSupervisorActor {
    val minBackoff = config.getDuration("surge.health.signal-supervisor-actor.backoff.min-backoff")
    val maxBackoff = config.getDuration("surge.health.signal-supervisor-actor.backoff.max-backoff")
    val randomFactor = config.getDouble("surge.health.signal-supervisor-actor.backoff.random-factor")
    val maxRetries = config.getInt("surge.health.signal-supervisor-actor.backoff.max-retries")

  }
}
