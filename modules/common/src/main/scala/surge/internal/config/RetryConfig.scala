// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.internal.config

import java.util.concurrent.TimeUnit

import com.typesafe.config.ConfigFactory

import scala.concurrent.duration._

object RetryConfig {
  private val config = ConfigFactory.load()

  object AggregateActor {
    val fetchStateRetryInterval: FiniteDuration =
      config.getDuration("surge.state-store-actor.fetch-state-retry-interval", TimeUnit.MILLISECONDS).milliseconds
    val initializeStateInterval: FiniteDuration =
      config.getDuration("surge.state-store-actor.initialize-state-retry-interval", TimeUnit.MILLISECONDS).milliseconds
    val maxInitializationAttempts: Int = config.getInt("surge.state-store-actor.max-initialization-attempts")
  }
}
