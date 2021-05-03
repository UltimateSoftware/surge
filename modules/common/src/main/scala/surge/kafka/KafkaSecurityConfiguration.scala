// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.kafka

import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.common.config.{ SaslConfigs, SslConfigs }

trait KafkaSecurityConfiguration {
  private val config = ConfigFactory.load()
  private def setIfDefined(props: Properties, key: String, configLocation: String): Unit = {
    if (config.hasPath(configLocation)) {
      val configValue = config.getString(configLocation)
      if (configValue.nonEmpty) {
        props.put(key, configValue)
      }
    }
  }

  def configureSecurityProperties(props: Properties): Unit = {
    setIfDefined(props, CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "kafka.security.protocol")

    setIfDefined(props, SaslConfigs.SASL_MECHANISM, "kafka.sasl.mechanism")
    setIfDefined(props, SaslConfigs.SASL_KERBEROS_SERVICE_NAME, "kafka.sasl.kerberos.service.name")
    setIfDefined(props, SaslConfigs.SASL_JAAS_CONFIG, "kafka.sasl.jaas.conf")

    setIfDefined(props, SslConfigs.SSL_KEY_PASSWORD_CONFIG, "kafka.ssl.key.password")
    setIfDefined(props, SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, "kafka.ssl.truststore.location")
    setIfDefined(props, SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "kafka.ssl.truststore.password")
    setIfDefined(props, SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, "kafka.ssl.keystore.location")
    setIfDefined(props, SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, "kafka.ssl.keystore.password")
  }
}
