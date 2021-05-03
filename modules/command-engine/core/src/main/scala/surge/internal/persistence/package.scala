// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal

import surge.core.SurgeCommandModel

package object persistence {
  type BusinessLogic = SurgeCommandModel[_, _, _, _]
}
