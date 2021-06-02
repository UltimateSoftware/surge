// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.persistence

import play.api.libs.json.JsValue
import surge.core.KafkaProducerActor
import surge.internal.persistence.PersistentActor.MetricsQuiver
import surge.kafka.streams.AggregateStateStoreKafkaStreams

case class PersistentEntitySharedResources(kafkaProducerActor: KafkaProducerActor, metrics: MetricsQuiver, stateStore: AggregateStateStoreKafkaStreams[JsValue])
