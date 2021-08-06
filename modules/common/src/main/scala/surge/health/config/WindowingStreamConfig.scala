// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.health.config

import com.typesafe.config.Config

import scala.concurrent.duration._

/**
 * WindowStreamConfig encapsulates all the configuration options for a WindowingHealthSignalStream
 * @param windowingDelay
 *   Initial Delay between start and the processing of window data.
 * @param maxWindowSize
 *   The maximum number of data elements in a window at any given time.
 * @param throttleConfig
 *   ThrottleConfig
 * @param advancerConfig
 *   WindowingStreamAdvancerConfig
 */
case class WindowingStreamConfig(
    windowingInitDelay: FiniteDuration,
    windowingResumeDelay: FiniteDuration,
    maxWindowSize: Int,
    throttleConfig: ThrottleConfig,
    advancerConfig: WindowingStreamAdvancerConfig)

trait WindowingStreamAdvancerConfigLoader[T] {
  def load(config: Config): T
}

sealed trait WindowingStreamAdvancerConfig {
  def advanceAmount: Int
  def buffer: Int
}

sealed trait ThrottleConfig {
  def elements: Int
  def duration: FiniteDuration
}

object ThrottleConfig {
  def apply(elements: Int, duration: FiniteDuration): ThrottleConfig = {
    ThrottleConfigImpl(elements, duration)
  }
}

private case class ThrottleConfigImpl(elements: Int, duration: FiniteDuration) extends ThrottleConfig

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
  def load(config: Config): WindowingStreamConfig = {
    val windowStreamConfig = config.getConfig("surge.health.window.stream")
    val initDelay = windowStreamConfig.getDuration("init-delay").toMillis.millis
    val resumeDelay = windowStreamConfig.getDuration("resume-delay").toMillis.millis
    val maxStreamSize = windowStreamConfig.getInt("max-size")
    //val frequencies = windowStreamConfig.getDurationList("frequencies").asScala.map(d => d.toMillis.millis)

    val throttleConfig = windowStreamConfig.getConfig("throttle")
    val windowStreamThrottleConfig = ThrottleConfig(throttleConfig.getInt("elements"), throttleConfig.getDuration("duration").toMillis.millis)
    val advancerConfig = windowStreamConfig.getConfig("advancer")
    val windowStreamAdvancerConfig = WindowingStreamAdvancerConfigLoader(advancerConfig.getString("type")).load(advancerConfig)

    WindowingStreamConfig(initDelay, resumeDelay, maxStreamSize, windowStreamThrottleConfig, windowStreamAdvancerConfig)
  }
}
