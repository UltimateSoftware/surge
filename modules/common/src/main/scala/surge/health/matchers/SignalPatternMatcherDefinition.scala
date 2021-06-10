// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.health.matchers

import java.util.regex.Pattern

import com.typesafe.config.Config
import surge.health.SignalType
import surge.health.domain.{ Error, HealthSignal, Trace, Warning }
import surge.internal.health.matchers.{ RepeatingSignalMatcher, SignalNameEqualsMatcher, SignalNamePatternMatcher }

import scala.jdk.CollectionConverters._
import scala.util.Try

trait SignalPatternMatcherDefinition {
  def toMatcher: SignalPatternMatcher
  def withSideEffect(sideEffect: SideEffect): SignalPatternMatcherDefinition
}

trait SignalPatternMatcherDefinitionFactory {
  def repeating(times: Int, pattern: Pattern, sideEffect: Option[SideEffect] = None): SignalPatternMatcherDefinition
  def nameEquals(signalName: String, sideEffect: Option[SideEffect] = None): SignalPatternMatcherDefinition
  def pattern(pattern: Pattern, sideEffect: Option[SideEffect] = None): SignalPatternMatcherDefinition
}

object SignalPatternMatcherDefinition extends SignalPatternMatcherDefinitionFactory {
  def fromConfig(config: Config, signalTopic: String): SignalPatternMatcherDefinition = {
    val sideEffect: Option[SideEffect] = Try {
      val sideEffectConfig = config.getConfig("side-effect")
      val signals = sideEffectConfig
        .getConfigList("signals")
        .asScala
        .map(sc => {
          val signalType = sc.getString("type")
          signalType match {
            case "error" =>
              HealthSignal(
                topic = signalTopic,
                name = sc.getString("name"),
                data = Error(sc.getString("description"), None, None),
                signalType = SignalType.ERROR)
            case "trace" =>
              HealthSignal(
                topic = signalTopic,
                name = sc.getString("name"),
                data = Trace(sc.getString("description"), None, None),
                signalType = SignalType.TRACE)
            case "warning" =>
              HealthSignal(
                topic = signalTopic,
                name = sc.getString("name"),
                data = Warning(sc.getString("description"), None, None),
                signalType = SignalType.WARNING)
          }
        })

      SideEffect(signals.toSeq)
    }.toOption

    config.getString("definitionType") match {
      case "repeating"  => repeating(times = config.getInt("times"), Pattern.compile(config.getString("pattern")), sideEffect)
      case "nameEquals" => nameEquals(config.getString("signalName"), sideEffect)
      case "pattern"    => pattern(Pattern.compile(config.getString("pattern")), sideEffect)
    }
  }

  override def nameEquals(signalName: String, sideEffect: Option[SideEffect] = None): SignalPatternMatcherDefinition = {
    SignalNameEqualsMatcherDefinition(signalName, sideEffect)
  }

  override def pattern(pattern: Pattern, sideEffect: Option[SideEffect] = None): SignalPatternMatcherDefinition =
    RegularExpressionSignalNameMatcherDefinition(pattern, sideEffect)

  override def repeating(times: Int, pattern: Pattern, sideEffect: Option[SideEffect] = None): SignalPatternMatcherDefinition =
    RepeatingSignalMatcherDefinition(times, pattern, sideEffect)

  protected case class SignalNameEqualsMatcherDefinition(signalName: String, sideEffect: Option[SideEffect] = None) extends SignalPatternMatcherDefinition {
    override def toMatcher: SignalPatternMatcher = SignalNameEqualsMatcher(signalName, sideEffect)

    override def withSideEffect(sideEffect: SideEffect): SignalPatternMatcherDefinition = copy(sideEffect = Some(sideEffect))
  }

  protected case class RegularExpressionSignalNameMatcherDefinition(pattern: Pattern, sideEffect: Option[SideEffect]) extends SignalPatternMatcherDefinition {
    override def toMatcher: SignalPatternMatcher = SignalNamePatternMatcher(pattern, sideEffect)

    override def withSideEffect(sideEffect: SideEffect): SignalPatternMatcherDefinition = copy(sideEffect = Some(sideEffect))
  }

  protected case class RepeatingSignalMatcherDefinition(times: Int, pattern: Pattern, sideEffect: Option[SideEffect] = None)
      extends SignalPatternMatcherDefinition {
    override def toMatcher: SignalPatternMatcher = RepeatingSignalMatcher(times, SignalNamePatternMatcher(pattern), sideEffect)

    override def withSideEffect(sideEffect: SideEffect): SignalPatternMatcherDefinition = copy(sideEffect = Some(sideEffect))
  }
}
