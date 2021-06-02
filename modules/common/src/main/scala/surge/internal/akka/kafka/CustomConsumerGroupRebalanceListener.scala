// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.akka.kafka

import akka.actor.{ Actor, ActorRef, Props }
import surge.kafka.PartitionAssignments

object CustomConsumerGroupRebalanceListener {
  def props(stateTrackingActor: ActorRef, updateCallback: PartitionAssignments => Unit): Props = {
    Props(new CustomConsumerGroupRebalanceListener(stateTrackingActor, updateCallback))
  }
}

/**
 * An actor wrapper to facilitate subscribing to the KTable consumer group tracking actor and forwarding any received updates to an arbitrary callback function
 * that can be used to listen for and respond to KTable consumer group updates in a custom way.
 * @param stateTrackingActor
 *   The ActorRef for the KafkaConsumerStateTrackingActor responsible for tracking updates to the KTable consumer group
 * @param updateCallback
 *   A function that is called whenever the KTable consumer group is updated with the newly updated partition assignments
 */
class CustomConsumerGroupRebalanceListener(stateTrackingActor: ActorRef, updateCallback: PartitionAssignments => Unit) extends Actor {
  override def preStart(): Unit = {
    stateTrackingActor ! KafkaConsumerStateTrackingActor.Register(self)
  }

  override def receive: Receive = { case assignments: PartitionAssignments =>
    updateCallback(assignments)
  }
}
