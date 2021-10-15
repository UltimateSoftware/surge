// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>
package surge.serialization

import play.api.libs.json.{ Format, Json }

class PlayJsonSerializer[TYPE]()(implicit format: Format[TYPE]) extends Serializer[TYPE] {
  override def serialize(data: TYPE): BytesPlusHeaders = {
    BytesPlusHeaders(Json.toJson(data).toString().getBytes())
  }
}
