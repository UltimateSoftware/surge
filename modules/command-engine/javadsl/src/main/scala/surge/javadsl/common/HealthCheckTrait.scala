// Copyright © 2017-2023 UKG Inc. <https://www.ukg.com>

//
package surge.javadsl.common

import java.util.concurrent.CompletionStage

trait HealthCheckTrait {
  def getHealthCheck: CompletionStage[HealthCheck]
  def getReadiness: CompletionStage[HealthCheck]
}
