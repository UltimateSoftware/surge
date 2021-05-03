// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.internal.kafka

import java.util

import org.apache.kafka.common.serialization.{ Deserializer, Serde, Serializer }
import play.api.libs.json.{ Format, Json, Reads, Writes }

class JsonSerializer[A](implicit writes: Writes[A]) extends org.apache.kafka.common.serialization.Serializer[A] {
  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}
  override def close(): Unit = {}

  override def serialize(topic: String, data: A): Array[Byte] = {
    Json.toJson(data).toString().getBytes()
  }
}
class JsonDeserializer[A >: Null](implicit reads: Reads[A]) extends org.apache.kafka.common.serialization.Deserializer[A] {
  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}
  override def close(): Unit = {}

  override def deserialize(topic: String, data: Array[Byte]): A = {
    Json.parse(data).asOpt[A].orNull
  }
}

// Can use this for deserializing json from kafka directly to a case class that has json formatting.
object JsonSerdes {
  private class JsonSerde[A >: Null](implicit format: Format[A]) extends Serde[A] {
    private val serializerImpl = new JsonSerializer[A]()
    private val deserializerImpl = new JsonDeserializer[A]()

    override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {
      serializerImpl.configure(configs, isKey)
      deserializerImpl.configure(configs, isKey)
    }
    override def close(): Unit = {
      serializerImpl.close()
      deserializerImpl.close()
    }

    override def serializer(): Serializer[A] = serializerImpl
    override def deserializer(): Deserializer[A] = deserializerImpl
  }

  implicit def serdeFor[A >: Null](implicit format: Format[A]): Serde[A] = {
    new JsonSerde[A]()
  }
  implicit def optionalSerdeFor[A](implicit format: Format[Option[A]]): Serde[Option[A]] = {
    new JsonSerde[Option[A]]()
  }
}
