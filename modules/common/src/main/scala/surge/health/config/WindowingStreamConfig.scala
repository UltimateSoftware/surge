// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.health.config

import com.typesafe.config.{ Config, ConfigFactory }

import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

/**
 * WindowStreamConfig encapsulates all the configuration options for a WindowingHealthSignalStream
 * @param windowingDelay
 *   Initial Delay between start and the processing of window data.
 * @param maxWindowSize
 *   The maximum number of data elements in a window at any given time.
 * @param frequencies
 *   The time to live for windows, a window will either advance or close or both when ttl expires.
 */
case class WindowingStreamConfig(
    windowingDelay: FiniteDuration,
    maxWindowSize: Int,
    frequencies: Seq[FiniteDuration],
    throttleConfig: WindowingStreamThrottleConfig,
    advancerConfig: WindowingStreamAdvancerConfig)

trait WindowingStreamAdvancerConfigLoader[T] {
  def load(config: Config): T
}

sealed trait WindowingStreamAdvancerConfig {
  def advanceAmount: Int
  def buffer: Int
}

sealed trait WindowingStreamThrottleConfig {
  def elements: Int
  def duration: FiniteDuration
}

object WindowingStreamThrottleConfig {
  def apply(elements: Int, duration: FiniteDuration): WindowingStreamThrottleConfig = {
    WindowingStreamThrottleConfigImpl(elements, duration)
  }
}

private case class WindowingStreamThrottleConfigImpl(elements: Int, duration: FiniteDuration) extends WindowingStreamThrottleConfig

case class WindowingStreamSliderConfig(buffer: Int, advanceAmount: Int) extends WindowingStreamAdvancerConfig

object WindowingStreamAdvancerConfigLoader {
  def apply(advancerType: String): WindowingStreamAdvancerConfigLoader[WindowingStreamSliderConfig] = {
    WindowingStreamSliderConfigLoader
  }
}

object WindowingStreamSliderConfigLoader extends WindowingStreamAdvancerConfigLoader[WindowingStreamSliderConfig] {
  def load(advancerConfig: Config): WindowingStreamSliderConfig = {
    val configuredSlideAmount: Int = advancerConfig.getInt("amount")
    val configuredBufferSize: Int = advancerConfig.getInt("buffer")

    WindowingStreamSliderConfig(configuredBufferSize, configuredSlideAmount)
  }
}

object WindowingStreamConfigLoader {
  private val config = ConfigFactory.load().getConfig("surge.health.window.stream")

  def load(config: Config): WindowingStreamConfig = {
    val maxDelay = config.getDuration("delay").toMillis.millis
    val maxStreamSize = config.getInt("max-size")
    val frequencies = config.getDurationList("frequencies").asScala.map(d => d.toMillis.millis)

    val throttleConfig = config.getConfig("throttle")
    val windowStreamThrottleConfig = WindowingStreamThrottleConfig(throttleConfig.getInt("elements"), throttleConfig.getDuration("duration").toMillis.millis)
    val advancerConfig = config.getConfig("advancer")
    val windowStreamAdvancerConfig = WindowingStreamAdvancerConfigLoader(advancerConfig.getString("type")).load(advancerConfig)

    WindowingStreamConfig(maxDelay, maxStreamSize, frequencies.toSeq, windowStreamThrottleConfig, windowStreamAdvancerConfig)
  }

  def load(): WindowingStreamConfig = {
    load(config)
  }
}
