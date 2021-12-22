// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.utils

import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers.convertToAnyMustWrapper
import org.slf4j.{ Logger, LoggerFactory, MDC }
import surge.internal.utils.MdcExecutionContext.mdcExecutionContext

import java.util.UUID
import scala.concurrent.Future

class DiagnosticContextFuturePropagationSpec extends AnyFlatSpec with ScalaFutures {
  val logger: Logger = LoggerFactory.getLogger(getClass.getSimpleName)

  "Child thread MDC context" should "be null when parent thread does not contain MDC context" in {
    val futureRequestId = Future {
      logger.info(s"future requestId propagated: ${MDC.get("requestId")}")
      MDC.get("requestId")
    }
    whenReady(futureRequestId) { requestId =>
      requestId mustEqual null
    }
  }

  "Child thread MDC context" should "equal to the parent thread MDC context" in {
    val id: String = UUID.randomUUID().toString
    MDC.put("requestId", id)
    val futureRequestId = Future {
      logger.info(s"future requestId propagated: ${MDC.get("requestId")}")
      MDC.get("requestId")
    }

    whenReady(futureRequestId) { requestId =>
      requestId mustEqual id
    }

    val doubleFutureRequestId = Future {
      "something"
    }.flatMap { _ =>
      Future {
        logger.info(s"future requestId propagated: ${MDC.get("requestId")}")
        MDC.get("requestId")
      }
    }

    whenReady(doubleFutureRequestId) { requestId =>
      requestId mustEqual id
    }
  }

}
