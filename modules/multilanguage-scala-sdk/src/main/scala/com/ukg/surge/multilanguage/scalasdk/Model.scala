// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>
package com.ukg.surge.multilanguage.scalasdk

import com.google.protobuf.ByteString

import scala.util.Try

final case class CQRSModel[S, E, C](eventHandler: (Option[S], E) => Option[S], commandHandler: (Option[S], C) => Either[String, Seq[E]]) {

  private[scalasdk] def applyEvents(s: Option[S], e: Seq[E]): Option[S] = {
    e.foldLeft(s)((s, e) => eventHandler(s, e))
  }

  private[scalasdk] def executeCommand(s: Option[S], c: C): Either[String, (Seq[E], Option[S])] = {
    commandHandler(s, c).map(events => (events, applyEvents(s, events)))
  }
}

final case class SerDeser[S, E, C](
    deserializeState: Array[Byte] => Try[S],
    deserializeEvent: Array[Byte] => Try[E],
    deserializeCommand: Array[Byte] => Try[C],
    serializeState: S => Try[Array[Byte]],
    serializeEvent: E => Try[Array[Byte]]) {

  def deserializeState(byteString: ByteString): Try[S] = {
    deserializeState(byteString.toByteArray)
  }

  def deserializeCommand(byteString: ByteString): Try[C] = {
    deserializeCommand(byteString.toByteArray)
  }

  def deserializeEvent(byteString: ByteString): Try[E] = {
    deserializeEvent(byteString.toByteArray)
  }

}
