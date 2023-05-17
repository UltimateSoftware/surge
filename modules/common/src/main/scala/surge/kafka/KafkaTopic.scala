// Copyright © 2017-2023 UKG Inc. <https://www.ukg.com>

package surge.kafka

trait KafkaTopicTrait {
  def name: String
}
object KafkaTopic {
  def of(name: String): KafkaTopic = {
    KafkaTopic(name)
  }
}

final case class KafkaTopic(name: String) extends KafkaTopicTrait
