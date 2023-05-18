// Copyright © 2017-2023 UKG Inc. <https://www.ukg.com>

package surge.health.jmx

import surge.health.jmx.Domain.HealthRegistrationDetail

object Api {
  case class AddComponent(detail: HealthRegistrationDetail)
  case class RemoveComponent(componentName: String)

  case class StopManagement()
  case class StartManagement()

  case class GetHealthRegistry()
}
