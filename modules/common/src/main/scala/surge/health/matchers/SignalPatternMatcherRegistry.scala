// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.health.matchers

import com.typesafe.config.{ Config, ConfigFactory }
import surge.health.config.SignalPatternMatcherConfig
import surge.health.matchers

import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.Try

/**
 * Maintains a collection of SignalPatternMatcher(s) that are applied to a HealthSignalStream to detect patterns.
 *
 * {{{
 *   surge {
 *     health {
 *         signal-pattern-matcher-registry = [
 *             {
 *                 definitionType = "repeating"
 *                 times = 5
 *                 pattern = "foo$"
 *                 side-effect = {
 *                     signals = [
 *                         {
 *                           type = "error"
 *                           name = "foobarError"
 *                           description = "foobar"
 *                         }
 *                     ]
 *                 }
 *             },
 *             {
 *                 definitionType = "nameEquals"
 *                 signalName = "foo"
 *                 side-effect = {
 *                     signals = [
 *                         {
 *                           type = "trace"
 *                           name = "foobarTrace"
 *                           description = "foobar"
 *                         }
 *                     ]
 *                 }
 *             },
 *             {
 *                 definitionType = "pattern"
 *                 pattern = "foo$"
 *                 side-effect = {
 *                     signals = [
 *                         {
 *                           type = "warning"
 *                           name = "foobarWarning"
 *                           description = "foobar"
 *                         }
 *                     ]
 *                 }
 *             }
 *         ]
 *     }
 * }
 * }}}
 */
object SignalPatternMatcherRegistry {
  private val signalPatternMatcherRegistryConfigField: String = "surge.health.signal-pattern-matcher-registry"
  private val healthSignalTopicConfigField: String = "surge.health.bus.signal-topic"

  def load(configName: Option[String] = None): SignalPatternMatcherRegistry = {
    val config: Config = ConfigFactory.load(configName.getOrElse("application"))

    val signalTopic = config.getString(healthSignalTopicConfigField)
    val defs: Seq[SignalPatternMatcherDefinition] = Try {
      val registryConfig = config.getConfigList(signalPatternMatcherRegistryConfigField)

      registryConfig.asScala
        .map(c => {
          SignalPatternMatcherDefinition.fromConfig(c, signalTopic)
        })
        .toSeq
    }.toOption.getOrElse(Seq[matchers.SignalPatternMatcherDefinition]())

    new SignalPatternMatcherRegistry().load(SignalPatternMatcherConfig(defs))
  }

  def apply(config: SignalPatternMatcherConfig = SignalPatternMatcherConfig(Seq.empty)): SignalPatternMatcherRegistry =
    new SignalPatternMatcherRegistry().load(config)
}

class SignalPatternMatcherRegistry {
  private val registry: mutable.Set[SignalPatternMatcher] = mutable.Set[SignalPatternMatcher]()

  def toSeq: Seq[SignalPatternMatcher] = registry.toSeq

  def definitionFactory: SignalPatternMatcherDefinitionFactory =
    SignalPatternMatcherDefinition

  def load(config: SignalPatternMatcherConfig): SignalPatternMatcherRegistry = {
    config.signalMatcherDefs.foreach(d => registry += d.toMatcher)
    this
  }
}
