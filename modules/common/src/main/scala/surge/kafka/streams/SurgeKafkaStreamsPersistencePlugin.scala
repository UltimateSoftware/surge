// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.kafka.streams

import com.typesafe.config.ConfigFactory
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier
import org.apache.kafka.streams.state.internals.RocksDbKeyValueBytesStoreSupplier
import org.slf4j.LoggerFactory

import scala.util.{ Failure, Success, Try }

trait SurgeKafkaStreamsPersistencePlugin {
  def createSupplier(storeName: String): KeyValueBytesStoreSupplier
  def enableLogging: Boolean
}

class RocksDBPersistencePlugin extends SurgeKafkaStreamsPersistencePlugin {
  override def createSupplier(storeName: String): KeyValueBytesStoreSupplier = {
    new RocksDbKeyValueBytesStoreSupplier(storeName, true)
  }
  override def enableLogging: Boolean = true
}

object SurgeKafkaStreamsPersistencePluginLoader {
  private val log = LoggerFactory.getLogger(getClass)
  private val config = ConfigFactory.load()
  private val persistencePluginName = config.getString("surge.kafka-streams.state-store-plugin")
  private val defaultPersistencePluginName = "rocksdb"
  private lazy val defaultPersistencePlugin = new RocksDBPersistencePlugin

  def load(): SurgeKafkaStreamsPersistencePlugin = {
    Try(config.getString(s"$persistencePluginName.plugin-class")) match {
      case Failure(_) ⇒
        log.error(s"Unable to find a config setting for $persistencePluginName.plugin-class. " +
          s"This means you've configured a Kafka Streams persistence plugin that does not exist or is not configured correctly. " +
          s"Falling back to the default setting of $defaultPersistencePluginName")
        defaultPersistencePlugin
      case Success(pluginClass) ⇒
        Try(Class.forName(pluginClass).newInstance()) match {
          case Success(value: SurgeKafkaStreamsPersistencePlugin) ⇒
            log.debug("Successfully loaded persistence plugin named [{}]", persistencePluginName)
            value
          case _ ⇒
            log.warn(s"Unable to find and instantiate a class named [$pluginClass] for plugin [$persistencePluginName]")
            defaultPersistencePlugin
        }
    }
  }
}
