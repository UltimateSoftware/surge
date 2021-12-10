// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.javadsl.command

import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatestplus.mockito.MockitoSugar
import surge.internal.domain.SurgeContext

import java.util
import java.util.Optional
import scala.util.control.NoStackTrace

class CommandModelsSpec extends AsyncWordSpec with Matchers with MockitoSugar with ScalaFutures {
  "AggregateCommandModel" should {
    "Return a failed future when the business logic throws an exception processing commands" in {
      val expectedException = new RuntimeException("This is expected") with NoStackTrace
      val exceptionThrowingModel = new AggregateCommandModel[String, String, String] {
        override def processCommand(aggregate: Optional[String], command: String): util.List[String] = throw expectedException
        override def handleEvent(aggregate: Optional[String], event: String): Optional[String] = Optional.empty()
      }
      recoverToSucceededIf[RuntimeException] {
        exceptionThrowingModel.toCore.handle(mock[SurgeContext[String, String]], None, "Test")
      }
    }
  }
}
