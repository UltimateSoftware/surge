// Copyright © 2017-2023 UKG Inc. <https://www.ukg.com>

package surge.health.config

import surge.health.matchers.SignalPatternMatcherDefinition

case class SignalPatternMatcherConfig(signalMatcherDefs: Seq[SignalPatternMatcherDefinition] = Seq.empty)
