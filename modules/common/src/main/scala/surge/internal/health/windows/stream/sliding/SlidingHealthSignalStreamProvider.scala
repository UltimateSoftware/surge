// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.health.windows.stream.sliding

import akka.actor.ActorSystem
import surge.health.HealthSignalStream
import surge.health.config.{ WindowingStreamConfig, WindowingStreamSliderConfig }
import surge.health.matchers.SignalPatternMatcher
import surge.internal.health._

/**
 * SlidingHealthSignalStreamProvider is responsible for providing a configured SlidingHealthSignalStream that is properly bound to a HealthSignalBus. A provided
 * collection of SignalPatternMatcher(s) are used to forward HealthSignals that match patterns defined by said SignalPatternMatcher(s).
 * @param config
 *   WindowingStreamConfig
 * @param actorSystem
 *   ActorSystem
 * @param streamMonitoring
 *   Option[StreamMonitoringRef]
 * @param filters
 *   Seq[SignalPatterMatcher]
 */
class SlidingHealthSignalStreamProvider(
    config: WindowingStreamConfig,
    override val actorSystem: ActorSystem,
    override val streamMonitoring: Option[StreamMonitoringRef] = None,
    override val filters: Seq[SignalPatternMatcher] = Seq.empty)
    extends HealthSignalStreamProvider {

  /**
   * Provide a SlidingHealthSignalStream with a defined set of filters.
   *
   * The filter semantics are inclusive; i.e. Include signals matching the filter test.
   * @param bus
   *   HealthSignalBusInternal
   * @return
   *   HealthSignalStream
   */
  override def provide(bus: HealthSignalBusInternal): HealthSignalStream = {
    if (config.advancerConfig.isInstanceOf[WindowingStreamSliderConfig]) {
      SlidingHealthSignalStream(config, bus, filters, streamMonitoring, actorSystem = actorSystem)
    } else {
      throw new RuntimeException(s"The Advancer Configuration provided in WindowingStreamConfig must be of type ${classOf[WindowingStreamSliderConfig]}")
    }
  }
}
