// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.utils

import akka.actor.{ Actor, ActorSystem, Props }
import akka.pattern.{ BackoffOpts, BackoffSupervisor }
import org.mockito.Mockito._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.{ Eventually, PatienceConfiguration }
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{ Millis, Seconds, Span }
import org.scalatest.wordspec.AnyWordSpecLike
import org.scalatestplus.mockito.MockitoSugar

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.{ FiniteDuration, _ }

class DoomedToCrash(crashIn: FiniteDuration)(implicit executionContext: ExecutionContext) extends Actor with Logging {

  case object Crash

  context.system.scheduler.scheduleOnce(crashIn, self, Crash)

  override def receive: Receive = {
    case Crash =>
      crash()
    case _ =>
    //
  }

  def crash(): Unit = {
    log.info(s"Actor ${self.path} is failing")
    throw new RuntimeException("This is expected")
  }

  override def preStart(): Unit = {
    log.info(s"Actor ${self.path} is starting")
    super.preStart()
  }

}

class NotificationReceiver {
  def onDie(): Unit = {}
}

class BackoffChildActorTerminationWatcherSpec
    extends AnyWordSpecLike
    with Matchers
    with MockitoSugar
    with BeforeAndAfterAll
    with Eventually
    with PatienceConfiguration {

  private val system = ActorSystem("test")

  override implicit val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(3, Seconds), interval = Span(10, Millis)) // scalastyle:ignore magic.number

  override def afterAll(): Unit = {
    system.terminate()
    super.beforeAll()
  }

  "BackoffChildActorTerminationWatcherSpec" should {
    "Notify when a supervised by a BackoffSupervisor actor dies" in {

      val dieIn: FiniteDuration = 200.millis
      val doomedToDieActorProps = Props(new DoomedToCrash(dieIn)(ExecutionContext.global))
      val notificationReceiver = mock[NotificationReceiver]

      val supervisorProps = BackoffSupervisor.props(
        BackoffOpts
          .onStop(doomedToDieActorProps, childName = "test", minBackoff = 200.millis, maxBackoff = 500.millis, randomFactor = 0.2)
          .withMaxNrOfRetries(2))

      val backoffSupervisor = system.actorOf(supervisorProps)

      system.actorOf(BackoffChildActorTerminationWatcher.props(backoffSupervisor, () => notificationReceiver.onDie()))

      eventually {
        verify(notificationReceiver, times(1)).onDie()
      }
    }

  }

}
