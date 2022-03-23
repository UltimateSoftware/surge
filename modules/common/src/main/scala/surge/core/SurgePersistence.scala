package surge.core

import akka.Done

import scala.concurrent.Future

trait SurgePersistence[State, Event] {
  def persistState(state: Option[State]): Future[Done]
  def fetchState(id: String): Future[Option[State]]
  def persistEvents(events: Seq[Event]): Future[Done]
  def persist(events: Seq[Event], state: Option[State]): Future[Done]
}
