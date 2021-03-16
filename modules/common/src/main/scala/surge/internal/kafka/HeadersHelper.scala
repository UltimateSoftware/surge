// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.internal.kafka

import org.apache.kafka.common.header.internals.{ RecordHeader, RecordHeaders }
import org.apache.kafka.common.header.{ Header, Headers }

import scala.jdk.CollectionConverters._

object HeadersHelper {
  def createHeaders(headerMap: Map[String, String]): Headers = {
    val headers: Seq[Header] = headerMap.map(kv => new RecordHeader(kv._1, kv._2.getBytes())).toSeq

    new RecordHeaders(headers.asJava)
  }

  def unapplyHeaders(headers: Headers): Map[String, Array[Byte]] = {
    headers.toArray.map(h => h.key() -> h.value()).toMap
  }
}
