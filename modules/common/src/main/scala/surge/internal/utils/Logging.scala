// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.internal.utils

import org.slf4j.LoggerFactory

trait Logging {
  val log = LoggerFactory.getLogger(getClass.getName)
}
