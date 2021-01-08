// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.scala.oss

import scala.util.Try

/**
 * UltiScala Domain Modeling Framework
 *
 * The intent of this package is to define a domain algebra modeling system.
 *
 * Domain models that implement this framework are completely abstracted
 * from any persistence or API layer.  This allows the developer to focus on the
 * functional correctness of the domain itself.
 *
 * Once developed, the model can be “injected” into any number of persistence
 * models:
 *
 *   * Lagom/Cassandra Command Service
 *   * Surge/Kafka Streams Command Service
 *   * MongoDB Query Service
 *   * MariaDB CRUD Service
 *   * etc
 *
 * Many of these could be based on prebuilt service chassis and deployment pipelines.
 * Domain models can then be easily stress tested under expected load scenarios
 * to determine the best persistence model for the service.
 *
 */
package object domain {
  /**
   * Defines a function type that tries applies a command to an aggregate
   *
   * @tparam Agg  The Aggregate Type
   * @tparam Cmd  The Aggregate's Base Command Type
   * @tparam Evt  The Aggregate's Base Event Type
   */
  type CommandProcessor[Agg, Cmd, Evt] = (Option[Agg], Cmd) => Try[Seq[Evt]]

  /**
   * Defines a function type that applies an event to an aggregate
   *
   * @tparam Agg  The Aggregate Type
   * @tparam Evt  The Aggregate's Base Event Type
   */
  type EventHandler[Agg, Evt] = (Option[Agg], Evt) => Option[Agg]
}
