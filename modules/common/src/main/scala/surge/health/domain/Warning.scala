// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.health.domain

case class Warning(description: String, error: Option[Throwable] = None, thread: Option[Thread] = None) extends BaseSignalData(description, error) {
  override def withError(error: Throwable): SignalData =
    this.copy(error = Some(error))
}
