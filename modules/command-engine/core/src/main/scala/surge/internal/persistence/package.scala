
package surge.internal

import surge.core.SurgeCommandModel

package object persistence {
  type BusinessLogic = SurgeCommandModel[_, _, _, _]
}
