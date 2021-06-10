// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.health.domain

import surge.health.SignalType

object HealthSignalBuilder {
  def apply(topic: String): HealthSignalBuilder =
    new HealthSignalBuilderImpl(topic)
}

trait HealthSignalBuilder {
  def withName(name: String): HealthSignalBuilder
  def withData(data: SignalData): HealthSignalBuilder

  def withSignalType(signalType: SignalType.Value): HealthSignalBuilder
  def withMetadata(metadata: Map[String, String]): HealthSignalBuilder

  def build(): HealthSignal
}

private[health] class HealthSignalBuilderImpl(topic: String) extends HealthSignalBuilder {
  private var name: String = _
  private var data: SignalData = _
  private var signalType: SignalType.Value = _
  private var metadata: Map[String, String] = _

  override def withName(name: String): HealthSignalBuilder = {
    this.name = name
    this
  }

  override def withData(data: SignalData): HealthSignalBuilder = {
    this.data = data
    this
  }

  override def withMetadata(metadata: Map[String, String]): HealthSignalBuilder = {
    this.metadata = metadata
    this
  }

  override def withSignalType(signalType: SignalType.Value): HealthSignalBuilder = {
    this.signalType = signalType
    this
  }

  override def build(): HealthSignal = {
    val meta: Map[String, String] = Option(metadata) match {
      case Some(data) => data
      case None =>
        Map.empty
    }

    val name = Option(this.name).getOrElse("unknown.health.signal")
    val typeOfSignal = Option(signalType).getOrElse(SignalType.OTHER)

    val signalData = Option(data).getOrElse(new SignalData {
      override def description: String = "no data provided"
      override def error: Option[Throwable] = None
      override def withError(error: Throwable): SignalData = this
    })

    val signal = HealthSignal(topic, name, typeOfSignal, signalData, meta)
    signal
  }
}
