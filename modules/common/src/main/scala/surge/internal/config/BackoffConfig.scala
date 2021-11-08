// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.config

import java.time.Duration

import com.typesafe.config.ConfigFactory

object BackoffConfig {
  private val config = ConfigFactory.load()

  object StateStoreKafkaStreamActor {
    val minBackoff: Duration = config.getDuration("surge.state-store-actor.backoff.min-backoff")
    val maxBackoff: Duration = config.getDuration("surge.state-store-actor.backoff.max-backoff")
    val randomFactor: Double = config.getDouble("surge.state-store-actor.backoff.random-factor")
    val maxRetries: Int = config.getInt("surge.state-store-actor.backoff.max-retries")
  }

  object HealthSignalWindowActor {
    val minBackoff: Duration = config.getDuration("surge.health.window-actors.backoff.min-backoff")
    val maxBackoff: Duration = config.getDuration("surge.health.window-actors.backoff.max-backoff")
    val randomFactor: Double = config.getDouble("surge.health.window-actors.backoff.random-factor")
    val maxRetries: Int = config.getInt("surge.health.window-actors.backoff.max-retries")
  }

  object HealthSupervisorActor {
    val minBackoff: Duration = config.getDuration("surge.health.supervisor-actor.backoff.min-backoff")
    val maxBackoff: Duration = config.getDuration("surge.health.supervisor-actor.backoff.max-backoff")
    val randomFactor: Double = config.getDouble("surge.health.supervisor-actor.backoff.random-factor")
    val maxRetries: Int = config.getInt("surge.health.supervisor-actor.backoff.max-retries")
  }
}
