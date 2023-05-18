// Copyright © 2017-2023 UKG Inc. <https://www.ukg.com>

package surge.internal.domain

sealed trait SurgeEngineStatus

object SurgeEngineStatus {
  case object Running extends SurgeEngineStatus
  case object Starting extends SurgeEngineStatus
  case object Stopped extends SurgeEngineStatus
}
