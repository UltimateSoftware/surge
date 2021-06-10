// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.health.matchers

import java.util.regex.Pattern

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import surge.health.config.SignalPatternMatcherConfig
import surge.internal.health.matchers.{ RepeatingSignalMatcher, SignalNameEqualsMatcher, SignalNamePatternMatcher }

class SignalPatternMatcherRegistrySpec extends AnyWordSpec with Matchers {

  "SignalPatternMatcherRegistry" should {
    "should load from config file" in {
      val registry = SignalPatternMatcherRegistry.load(Some("signal-pattern-matcher-registry"))

      registry.toSeq.exists(m => m.isInstanceOf[RepeatingSignalMatcher]) shouldEqual true
      registry.toSeq.exists(m => m.isInstanceOf[SignalNameEqualsMatcher]) shouldEqual true
      registry.toSeq.exists(m => m.isInstanceOf[SignalNamePatternMatcher]) shouldEqual true

      registry.toSeq.count(p => p.sideEffect().isDefined) shouldEqual 3
    }

    "should load from config" in {
      val registry = SignalPatternMatcherRegistry(
        SignalPatternMatcherConfig(Seq(
          SignalPatternMatcherDefinition.repeating(times = 5, pattern = Pattern.compile("foo$")),
          SignalPatternMatcherDefinition.nameEquals(signalName = "foo"),
          SignalPatternMatcherDefinition.pattern(Pattern.compile("foo$")))))

      registry.toSeq.exists(m => m.isInstanceOf[RepeatingSignalMatcher]) shouldEqual true
      registry.toSeq.exists(m => m.isInstanceOf[SignalNameEqualsMatcher]) shouldEqual true
      registry.toSeq.exists(m => m.isInstanceOf[SignalNamePatternMatcher]) shouldEqual true
    }

    "should create repeating def" in {
      val registry = SignalPatternMatcherRegistry()
      val factory: SignalPatternMatcherDefinitionFactory = registry.definitionFactory

      factory.repeating(times = 5, Pattern.compile("foo")).toMatcher shouldBe a[RepeatingSignalMatcher]
    }

    "should create nameEquals def" in {
      val registry = SignalPatternMatcherRegistry()
      val factory: SignalPatternMatcherDefinitionFactory = registry.definitionFactory
      factory.nameEquals(signalName = "foo").toMatcher shouldBe a[SignalNameEqualsMatcher]
    }

    "should create pattern def" in {
      val registry = SignalPatternMatcherRegistry()
      val factory: SignalPatternMatcherDefinitionFactory = registry.definitionFactory
      factory.pattern(Pattern.compile("some pattern")).toMatcher shouldBe a[SignalNamePatternMatcher]
    }
  }
}
