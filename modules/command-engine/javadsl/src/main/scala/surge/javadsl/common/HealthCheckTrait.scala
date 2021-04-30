//
package surge.javadsl.common

import java.util.concurrent.CompletionStage

trait HealthCheckTrait {
  def getHealthCheck: CompletionStage[HealthCheck]
}
