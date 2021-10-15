// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.core

trait SerializedWrapperSupport {
  def asSerializedMessage(key: String): SerializedMessage
  def asSerializedAggregate(): SerializedAggregate
}
