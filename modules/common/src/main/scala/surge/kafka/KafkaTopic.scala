// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.kafka

object KafkaTopic {
  def of(name: String): KafkaTopic = {
    KafkaTopic(name)
  }
}

final case class KafkaTopic(name: String)
