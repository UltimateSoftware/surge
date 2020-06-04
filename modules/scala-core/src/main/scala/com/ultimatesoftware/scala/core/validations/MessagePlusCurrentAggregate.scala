// Copyright © 2017-2019 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.scala.core.validations

final case class MessagePlusCurrentAggregate[+A, B](msg: A, aggregate: Option[B])
