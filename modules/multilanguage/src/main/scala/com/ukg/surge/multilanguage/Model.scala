// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package com.ukg.surge.multilanguage

import com.google.protobuf.ByteString

import scala.language.implicitConversions

// We need a fix in core Surge to get rid of these case classes
// For some reason, Surge doesn't allow us to serialize protobuf

case class SurgeState(payload: Array[Byte]) {}

case class SurgeEvent(aggregateId: String, payload: Array[Byte]) {}

case class SurgeCmd(aggregateId: String, payload: Array[Byte]) {}

object Implicits {

  //
  // Once we get rid of the case classes above, the implicit convs
  // are not going to be needed
  //

  implicit def byteArrayToByteString(byteArray: Array[Byte]): ByteString = {
    ByteString.copyFrom(byteArray)
  }

  implicit def byteStringToByteArray(byteString: ByteString): Array[Byte] = {
    byteString.toByteArray
  }

  implicit def surgeStateToPbState(state: SurgeState): protobuf.State = {
    protobuf.State(payload = state.payload)
  }

  implicit def surgeEventToPbEvent(event: SurgeEvent): protobuf.Event = {
    protobuf.Event(aggregateId = event.aggregateId, payload = event.payload)
  }

  implicit def surgeCommandToPbCommand(command: SurgeCmd): protobuf.Command = {
    protobuf.Command(aggregateId = command.aggregateId, payload = command.payload)
  }

  implicit def pbEventToSurgeEvent(event: protobuf.Event): SurgeEvent = {
    SurgeEvent(event.aggregateId, payload = event.payload.toByteArray)
  }

  implicit def pbStateToSurgeState(state: protobuf.State): SurgeState = {
    SurgeState(state.payload.toByteArray)
  }

  implicit def pbCommandToSurgeCmd(command: protobuf.Command): SurgeCmd = {
    SurgeCmd(command.aggregateId, payload = command.payload)
  }

}
