// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.persistence

import surge.core.KafkaProducerActor
import surge.internal.persistence.PersistentActor.MetricsQuiver
import surge.kafka.streams.AggregateStateStoreKafkaStreams

case class PersistentEntitySharedResources(
    aggregateIdToKafkaProducer: String => KafkaProducerActor,
    metrics: MetricsQuiver,
    stateStore: AggregateStateStoreKafkaStreams)
