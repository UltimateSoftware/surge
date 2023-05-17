// Copyright © 2017-2023 UKG Inc. <https://www.ukg.com>

package surge.javadsl.event

import org.apache.kafka.common.TopicPartition
import surge.kafka.HostPort

trait ConsumerRebalanceListener[AggId, Agg, Evt] {
  def onRebalance(engine: SurgeEvent[AggId, Agg, Evt], assignments: java.util.Map[HostPort, java.util.List[TopicPartition]]): Unit
}
