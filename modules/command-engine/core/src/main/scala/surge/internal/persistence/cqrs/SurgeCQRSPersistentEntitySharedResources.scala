// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.internal.persistence.cqrs

import play.api.libs.json.JsValue
import surge.core.KafkaProducerActor
import surge.internal.persistence.cqrs.CQRSPersistentActor.CQRSPersistentActorMetrics
import surge.kafka.streams.AggregateStateStoreKafkaStreams

case class SurgeCQRSPersistentEntitySharedResources(
    kafkaProducerActor: KafkaProducerActor,
    metrics: CQRSPersistentActorMetrics,
    kafkaStreamsCommand: AggregateStateStoreKafkaStreams[JsValue])
