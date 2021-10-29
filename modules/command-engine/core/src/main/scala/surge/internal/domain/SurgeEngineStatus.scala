package surge.internal.domain

sealed trait SurgeEngineStatus

object SurgeEngineStatus {
  case object On extends SurgeEngineStatus
  case object Off extends SurgeEngineStatus
}
