// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>
package surge.internal.kafka

import org.apache.kafka.common.utils.Utils

object PartitionerHelper {

  def partitionForKey(key: String, numberOfPartitions: Int): Int = {
    // simplified version of Kafka's `DefaultPartitioner` implementation
    val partition = org.apache.kafka.common.utils.Utils.toPositive(Utils.murmur2(key.getBytes())) % numberOfPartitions
    partition
  }

}
