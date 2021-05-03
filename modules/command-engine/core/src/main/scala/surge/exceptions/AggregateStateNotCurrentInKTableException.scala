// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.exceptions

import org.apache.kafka.common.TopicPartition

case class AggregateStateNotCurrentInKTableException(aggregateId: String, partition: TopicPartition)
    extends RuntimeException(
      s"Most recent state for aggregate $aggregateId is not yet present in the KTable view. " +
        s"Check the partition lag for topic/partition ${partition.topic()}:${partition.partition()} to monitor progress of the KTable indexing.")
