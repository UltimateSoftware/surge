// Copyright © 2017-2023 UKG Inc. <https://www.ukg.com>

package surge.health.domain

case class Error(override val description: String, override val error: Option[Throwable], thread: Option[Thread] = None)
    extends BaseSignalData(description, error) {
  override def withError(error: Throwable): SignalData =
    this.copy(error = Some(error))
}
