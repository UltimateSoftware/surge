// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.utils

import scala.util.control.NoStackTrace

class ExpectedTestException extends RuntimeException("This is expected") with NoStackTrace
