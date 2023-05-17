// Copyright © 2017-2023 UKG Inc. <https://www.ukg.com>

package surge.core.commondsl

import surge.internal.domain.SurgeProcessingModel

trait SurgeProcessingModelCoreTrait[S, M, E] {
  def toCore: SurgeProcessingModel[S, M, E]
}
