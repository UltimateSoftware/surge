// Copyright © 2017-2023 UKG Inc. <https://www.ukg.com>

package surge.kafka.streams

import java.util

import com.typesafe.config.ConfigFactory
import org.apache.kafka.streams.state.RocksDBConfigSetter
import org.rocksdb.{ BlockBasedTableConfig, InfoLogLevel, Options, Statistics, StatsLevel }
import org.slf4j.LoggerFactory

trait BlockCacheSettings {
  def blockSizeKb: Int
  def blockCacheSizeMb: Int
  def cacheIndexAndFilterBlocks: Boolean
}

trait WriteBufferSettings {
  def maxWriteBufferNumber: Int
  def writeBufferSizeMb: Int
}

object CustomRocksDBConfigSetter {
  private val config = ConfigFactory.load()

  val rocksCompactionParallelism: Int = config.getInt("kafka.streams.rocks-db.compaction-parallelism")
  val dumpStatistics: Boolean = config.getBoolean("kafka.streams.rocks-db.dump-statistics")
  val statisticsInterval: Int = config.getInt("kafka.streams.rocks-db.statistics-interval-seconds")
}

/**
 * By default, RocksDB is used as the backing for a Kafka Streams KTable. This class can be used to configure settings used by the embedded RocksDB instance
 * responsible for storing the data. This class additionally pulls in config settings for additional RocksDB configuration. See the `kafka.streams.rocks-db`
 * section of reference.conf for a complete list of settings this class looks at. This should be extended by another class and configured in the Kafka Streams
 * settings, ex: class MyCustomRocksDBSettings extends CustomRocksDbConfigSetter(..., ...)
 *
 * val streamsSettings = Map( ... StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG -> classOf[MyCustomRocksDBSettings].getName ... )
 *
 * @param blockCacheSettings
 *   Settings for the RocksDB block cache
 * @param writeBufferSettings
 *   Settings for the RocksDB write buffer
 */
abstract class CustomRocksDBConfigSetter(blockCacheSettings: BlockCacheSettings, writeBufferSettings: WriteBufferSettings) extends RocksDBConfigSetter {
  import CustomRocksDBConfigSetter._
  private val log = LoggerFactory.getLogger(getClass)

  override def setConfig(storeName: String, options: Options, configs: util.Map[String, AnyRef]): Unit = {
    options.tableFormatConfig() match {
      case tableConfig: BlockBasedTableConfig =>
        tableConfig.setBlockCacheSize(blockCacheSettings.blockCacheSizeMb * 1024 * 1024L)
        tableConfig.setBlockSize(blockCacheSettings.blockSizeKb * 1024L)
        tableConfig.setCacheIndexAndFilterBlocks(blockCacheSettings.cacheIndexAndFilterBlocks)
        tableConfig.setPinL0FilterAndIndexBlocksInCache(true)
      case _ =>
        log.warn(
          "RocksDB tableFormatConfig was not of type BlockBasedTableConfig. " +
            "Ignoring any configured block cache settings since they wouldn't impact anything.")
    }

    options.setMaxWriteBufferNumber(writeBufferSettings.maxWriteBufferNumber)
    options.setWriteBufferSize(writeBufferSettings.writeBufferSizeMb * 1024 * 1024L)

    val parallelThreads = Runtime.getRuntime.availableProcessors()
    val compactionParallelism = Math.max(parallelThreads, rocksCompactionParallelism)
    options.setMaxBackgroundCompactions(compactionParallelism)
    options.setLevelCompactionDynamicLevelBytes(true)

    if (dumpStatistics) {
      val stats = new Statistics()
      stats.setStatsLevel(StatsLevel.ALL)
      options.setInfoLogLevel(InfoLogLevel.INFO_LEVEL)
      options.setStatistics(stats)
      options.setStatsDumpPeriodSec(statisticsInterval)
    }
  }

  override def close(storeName: String, options: Options): Unit = {}
}
