// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.health.domain

trait SignalData {
  def description: String
  def error: Option[Throwable]
  def withError(error: Throwable): SignalData
}

abstract class BaseSignalData(description: String, error: Option[Throwable]) extends SignalData
