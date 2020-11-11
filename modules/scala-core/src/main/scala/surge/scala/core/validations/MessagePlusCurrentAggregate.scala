// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.scala.core.validations

final case class MessagePlusCurrentAggregate[+A, B](msg: A, aggregate: Option[B])
