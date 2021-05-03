// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.utils

import org.slf4j.LoggerFactory

trait Logging {
  val log = LoggerFactory.getLogger(getClass.getName)
}
