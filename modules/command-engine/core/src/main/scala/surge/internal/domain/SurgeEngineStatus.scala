package surge.internal.domain

sealed trait SurgeEngineStatus

object SurgeEngineStatus {
  case object Running extends SurgeEngineStatus
  case object Stopped extends SurgeEngineStatus
}
