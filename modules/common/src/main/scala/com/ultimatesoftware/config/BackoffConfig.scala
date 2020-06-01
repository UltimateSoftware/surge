// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.config

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
}
