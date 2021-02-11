// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.metrics.statistics

import java.time.Instant

import org.scalatest.Assertion
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import surge.metrics.MetricValueProvider

trait StatisticsSpec extends AnyWordSpecLike with Matchers {

  class TestContext(metricValueProvider: MetricValueProvider) {
    def recordValues(values: Seq[Double]): Unit = values.foreach(recordValue)
    def recordValue(value: Double): Unit = recordValue(value, Instant.now.toEpochMilli)
    def recordValue(value: Double, timestampMs: Long): Unit = metricValueProvider.update(value, timestampMs)

    def expectValue(expectedValue: Double): Assertion = {
      metricValueProvider.getValue shouldEqual expectedValue
    }
  }

  def testForProvider[T](valueProvider: MetricValueProvider)(testBody: TestContext => T): T = {
    testBody(new TestContext(valueProvider))
  }

  case class ProviderTestData(metricValueProvider: MetricValueProvider, recordedValues: Seq[Double], expectedValue: Double)
  def testMultipleStatisticInstances[T](testCases: Seq[ProviderTestData]): Unit = {
    testCases.foreach { testCase =>
      testForProvider(testCase.metricValueProvider) { context =>
        context.recordValues(testCase.recordedValues)
        context.expectValue(testCase.expectedValue)
      }
    }
  }
}
