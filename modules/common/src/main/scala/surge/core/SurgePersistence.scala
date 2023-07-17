package surge.core

import akka.Done

import scala.concurrent.Future

trait SurgeSnapshotting[State] {
  def persistState(state: Option[State]): Future[Done]
  def fetchState(id: String): Future[Option[State]]
}

trait SurgeEventPublishing[State, Event] extends SurgeSnapshotting[State] {
  def persistEvents(events: Seq[Event]): Future[Done]
  def persistAll(events: Seq[Event], state: Option[State]): Future[Done]
}
