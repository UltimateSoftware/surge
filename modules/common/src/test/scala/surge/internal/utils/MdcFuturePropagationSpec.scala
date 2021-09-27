package surge.internal.utils

import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers.convertToAnyMustWrapper
import org.slf4j.{ Logger, LoggerFactory, MDC }
import surge.internal.utils.MdcExecutionContext.mdcExecutionContext

import java.util.UUID
import scala.concurrent.Future

class MdcFuturePropagationSpec extends AnyFlatSpec with ScalaFutures {
  val logger: Logger = LoggerFactory.getLogger(getClass.getSimpleName)

  "Child thread context for logging" should "be null when parent thread does not contain context for logging" in {
    val futureRequestId = Future {
      logger.info(s"future requestId propagated: ${MDC.get("requestId")}")
      MDC.get("requestId")
    }
    whenReady(futureRequestId) { requestId =>
      requestId mustEqual null
    }

  }

  "Child thread context for logging" should "equal to tne parent thread context for logging" in {
    val id: String = UUID.randomUUID().toString
    MDC.put("requestId", id)
    val futureRequestId = Future {
      logger.info(s"future requestId propagated: ${MDC.get("requestId")}")
      MDC.get("requestId")
    }

    whenReady(futureRequestId) { requestId =>
      requestId mustEqual id
    }

  }

}
