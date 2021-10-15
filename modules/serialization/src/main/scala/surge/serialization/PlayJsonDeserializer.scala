// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.serialization

import play.api.libs.json.{ Format, Json }

class PlayJsonDeserializer[TYPE]()(implicit format: Format[TYPE]) extends Deserializer[TYPE] {
  override def deserialize(body: Array[Byte]): TYPE = Json.parse(body).as[TYPE]
}
