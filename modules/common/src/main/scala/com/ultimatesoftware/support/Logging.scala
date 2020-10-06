// Copyright © 2018-2020 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.support

import org.slf4j.LoggerFactory

trait Logging {
  val log = LoggerFactory.getLogger(getClass.getName)
}
