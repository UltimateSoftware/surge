// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.javadsl.event

import org.apache.kafka.common.TopicPartition
import surge.kafka.HostPort

trait ConsumerRebalanceListener[AggId, Agg, Evt, Response] {
  def onRebalance(engine: SurgeEvent[AggId, Agg, Evt, Response], assignments: java.util.Map[HostPort, java.util.List[TopicPartition]]): Unit
}
