// Copyright © 2017-2023 UKG Inc. <https://www.ukg.com>

package surge.internal.kafka

import surge.kafka.KafkaTopic

trait SurgeKafkaConfig {
  def stateTopic: KafkaTopic

  def eventsTopicOpt: Option[KafkaTopic]

  def streamsApplicationId: String

  def clientId: String

  def transactionalIdPrefix: String
}
