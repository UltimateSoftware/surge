// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.metrics

import java.util.UUID
import play.api.libs.json.{ Format, Json }

final case class Metric(
    name: String,
    value: Double = 0d,
    tenantId: Option[UUID] = None,
    additionalInfo: Option[Map[String, String]] = None)

object Metric {
  implicit lazy val format: Format[Metric] = Json.format
}
