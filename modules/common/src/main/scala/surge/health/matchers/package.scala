// Copyright © 2017-2023 UKG Inc. <https://www.ukg.com>

package surge.health

/**
 * Provides classes for matching signals in a HealthSignalStream.
 *
 * Building a SignalPatternMatcherRegistry
 * {{{
 * scala> val registry = SignalPatternMatcherRegistry(
 *         SignalPatternMatcherConfig(Seq(
 *           SignalPatternMatcherDefinition.repeating(times = 5, pattern = Pattern.compile("foo$")),
 *           SignalPatternMatcherDefinition.nameEquals(signalName = "foo"),
 *           SignalPatternMatcherDefinition.pattern(Pattern.compile("foo$")))))
 * }}}
 *
 * Loading a SignalPatternMatcherRegistry from a specific config file
 * {{{
 * scala> val registry = SignalPatternMatcherRegistry.load(Some("signal-pattern-matcher-registry"))
 * }}}
 *
 * Loading a SignalPatternMatcherRegistry from the default config file
 * {{{
 * scala> val registry = SignalPatternMatcherRegistry.load()
 * }}}
 */
package object matchers {}
