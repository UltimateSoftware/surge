// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.config

import com.typesafe.config.Config

import java.util.concurrent.TimeUnit
import scala.concurrent.duration._

class RetryConfig(config: Config) {

  object AggregateActor {
    val fetchStateRetryInterval: FiniteDuration =
      config.getDuration("surge.state-store-actor.fetch-state-retry-interval", TimeUnit.MILLISECONDS).milliseconds
    val initializeStateInterval: FiniteDuration =
      config.getDuration("surge.state-store-actor.initialize-state-retry-interval", TimeUnit.MILLISECONDS).milliseconds
    val maxInitializationAttempts: Int = config.getInt("surge.state-store-actor.max-initialization-attempts")
  }
}
