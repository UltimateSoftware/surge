// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.metrics

import java.lang.management.{ ManagementFactory, MemoryMXBean, OperatingSystemMXBean, ThreadMXBean }

final case class SnapshotSystemMetrics(metricsProvider: MetricsProvider) extends AutoUpdatingMetrics {

  lazy val freeMemoryGauge: Gauge = metricsProvider.createGauge("free_memory", None)
  lazy val memoryUsageGauge: Gauge = metricsProvider.createGauge("memory_usage", None)
  lazy val systemLoadGauge: Gauge = metricsProvider.createGauge("system_load", None)
  lazy val threadCountGauge: Gauge = metricsProvider.createGauge("thread_count", None)
  lazy val peakThreadCountGauge: Gauge = metricsProvider.createGauge("peak_thread_count", None)

  lazy val allMetrics: Seq[MetricContainerTrait[_]] = Seq(freeMemoryGauge, memoryUsageGauge, systemLoadGauge, threadCountGauge, peakThreadCountGauge)

  def snapshotSysMetrics: Seq[MetricContainerTrait[_]] = {
    val freeMem = Runtime.getRuntime.freeMemory()
    val totMem = Runtime.getRuntime.totalMemory()

    freeMemoryGauge.set(freeMem.toDouble)
    memoryUsageGauge.set((totMem - freeMem).toDouble / totMem.toDouble)
    systemLoadGauge.set(SnapshotSystemMetrics.osBean.getSystemLoadAverage)
    threadCountGauge.set(SnapshotSystemMetrics.threadBean.getThreadCount)
    peakThreadCountGauge.set(SnapshotSystemMetrics.threadBean.getPeakThreadCount)

    allMetrics
  }

  def update(): Unit = {
    snapshotSysMetrics
  }
}

object SnapshotSystemMetrics {
  val osBean: OperatingSystemMXBean = ManagementFactory.getOperatingSystemMXBean
  val memBean: MemoryMXBean = ManagementFactory.getMemoryMXBean
  val threadBean: ThreadMXBean = ManagementFactory.getThreadMXBean
}
