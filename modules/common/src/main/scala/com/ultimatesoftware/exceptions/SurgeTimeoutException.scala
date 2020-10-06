// Copyright © 2018-2020 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.exceptions

import java.util.concurrent.TimeoutException

case class SurgeTimeoutException(msg: String) extends TimeoutException(msg)
