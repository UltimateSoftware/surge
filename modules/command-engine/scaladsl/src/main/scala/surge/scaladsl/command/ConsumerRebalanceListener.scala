// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.scaladsl.command

import org.apache.kafka.common.TopicPartition
import surge.kafka.HostPort

trait ConsumerRebalanceListener[AggId, Agg, Command, Rej, Evt, Response] {
  def onRebalance(engine: SurgeCommand[AggId, Agg, Command, Rej, Evt, Response], assignments: Map[HostPort, List[TopicPartition]]): Unit
}
