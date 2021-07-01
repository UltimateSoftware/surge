// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.utils

import akka.Done
import akka.actor.{ ActorRef, ActorSystem }
import akka.testkit.{ EventFilter, ImplicitSender, TestKit }
import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.duration._
import scala.concurrent.{ ExecutionContext, Future }
import scala.language.postfixOps

class ExecutionContextProberSpec
    extends TestKit(ActorSystem("ExecutionContextProberSpec", ConfigFactory.parseString("""akka.loggers = ["akka.testkit.TestEventListener"]""")))
    with ImplicitSender
    with AnyWordSpecLike
    with Matchers
    with BeforeAndAfterAll
    with ScalaFutures {

  import system.dispatcher

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system, duration = 15 seconds, verifySystemShutdown = true)
  }

  def createProberActor(targetEc: ExecutionContext, interval: FiniteDuration = 7 seconds, name: String): ActorRef = {
    system.actorOf(
      ExecutionContextProberActor
        .props(
          ExecutionContextProberSettings(
            targetEc = targetEc,
            targetEcName = "test-dispatcher",
            initialDelay = 100 millis,
            timeout = 50 millis,
            interval,
            numProbes = 7))
        .withDispatcher("execution-context-prober.dispatcher"),
      name)
  }

  "The Execution Context Prober" should {

    "detect a 'starved' execution context and log a warning" in {

      val starvedEc: ExecutionContext = new ExecutionContext {
        override def execute(runnable: Runnable): Unit = {
          // we do nothing
        }
        override def reportFailure(cause: Throwable): Unit = ()
      }

      val prober = createProberActor(starvedEc, name = "prober-1")

      EventFilter
        .warning(pattern = s".*${ExecutionContextProberActor.warningText}.*", occurrences = 1)
        .intercept {} // waits max 3 seconds (akka.test.filter-leeway)

      prober ! ExecutionContextProberActor.Messages.HasIssues
      expectMsg(true)

      prober ! ExecutionContextProberActor.Messages.Stop
      expectMsg(Done)
    }

    "not report a healthy execution context as starved" in {
      val prober = createProberActor(targetEc = system.dispatcher, interval = 1 second, name = "prober-2")

      // wait 3 seconds
      akka.pattern.after(3 seconds) {
        Future {
          prober ! ExecutionContextProberActor.Messages.HasIssues
        }
      }
      expectMsg(max = 4 seconds, false)

      prober ! ExecutionContextProberActor.Messages.Stop
      expectMsg(Done)
    }

  }

  "The Execution Context Prober extension".can {

    "start / stop successfully" in {
      val proberExtension = ExecutionContextProber(system)
      proberExtension.stop().futureValue shouldBe Done
    }

  }

}
