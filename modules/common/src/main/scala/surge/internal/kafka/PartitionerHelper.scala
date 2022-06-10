// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.kafka

import scala.util.hashing.MurmurHash3

object PartitionerHelper {

  def partitionForKey(key: String, numberOfPartitions: Int): Int = {
    val partition = math.abs(MurmurHash3.stringHash(key) % numberOfPartitions)
    partition
  }

}
