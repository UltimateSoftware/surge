// Copyright © 2017-2023 UKG Inc. <https://www.ukg.com>

package surge.health.jmx

object Domain {
  case class HealthRegistrationDetail(componentName: String, controlRefPath: String)
}
