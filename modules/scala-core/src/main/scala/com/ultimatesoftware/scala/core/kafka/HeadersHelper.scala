// Copyright © 2018-2020 Ultimate Software Group. <https://www.ultimatesoftware.com>

package com.ultimatesoftware.scala.core.kafka

import org.apache.kafka.common.header.{ Header, Headers }
import org.apache.kafka.common.header.internals.{ RecordHeader, RecordHeaders }

import scala.collection.JavaConverters._

object HeadersHelper {
  def createHeaders(headerMap: Map[String, String]): Headers = {
    val headers: Seq[Header] = headerMap.map(kv ⇒ new RecordHeader(kv._1, kv._2.getBytes())).toSeq

    new RecordHeaders(headers.asJava)
  }
}
