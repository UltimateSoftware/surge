// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.rabbit

import io.arivera.oss.embedded.rabbitmq.EmbeddedRabbitMqConfig

object UriInfo {
  def fromConfig(config: EmbeddedRabbitMqConfig, user: String, vhost: String): UriInfo = {
    val host: String = "localhost"
    val port: Int = config.getRabbitMqPort

    UriInfo(s"amqp://$user:$user@$host:$port/$vhost", host, port)
  }
}

case class UriInfo(uri: String, host: String, port: Int)

