// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.exceptions

case class KafkaPublishTimeoutException(aggregateId: String, cause: Throwable)
    extends RuntimeException(s"Aggregate $aggregateId timed out trying to publish to Kafka", cause)
