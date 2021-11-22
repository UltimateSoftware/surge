// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package docs.command

import java.util.UUID

import org.apache.kafka.common.TopicPartition
import surge.kafka.HostPort
import surge.scaladsl.command.{ ConsumerRebalanceListener, SurgeCommand }

// #rebalance_listener
class BankAccountRebalanceListener extends ConsumerRebalanceListener[UUID, BankAccount, BankAccountCommand, BankAccountEvent] {
  override def onRebalance(
      engine: SurgeCommand[UUID, BankAccount, BankAccountCommand, BankAccountEvent],
      assignments: Map[HostPort, List[TopicPartition]]): Unit = {
    // Handle any updates to partition assignments here
  }
}
// #rebalance_listener
