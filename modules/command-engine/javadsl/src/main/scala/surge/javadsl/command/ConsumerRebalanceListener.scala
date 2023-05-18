// Copyright © 2017-2023 UKG Inc. <https://www.ukg.com>

package surge.javadsl.command

import org.apache.kafka.common.TopicPartition
import surge.kafka.HostPort

trait ConsumerRebalanceListener[AggId, Agg, Command, Evt] {
  def onRebalance(engine: SurgeCommand[AggId, Agg, Command, Evt], assignments: java.util.Map[HostPort, java.util.List[TopicPartition]]): Unit
}
