// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.exceptions

case class AggregateInitializationException(aggregateId: String, cause: Throwable)
    extends RuntimeException(s"Unable to fetch aggregate state for aggregate $aggregateId", cause)
