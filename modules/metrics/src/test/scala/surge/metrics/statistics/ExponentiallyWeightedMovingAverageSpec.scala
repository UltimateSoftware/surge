// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.metrics.statistics

class ExponentiallyWeightedMovingAverageSpec extends StatisticsSpec {
  "ExponentiallyWeightedMovingAverage" should {
    "Properly track the minimum observed value" in {
      testForProvider(new ExponentiallyWeightedMovingAverage(0.5)) { context =>
        import context._

        expectValue(0.0)
        recordValues(Seq(10.0, 5.0, 10.0))
        expectValue(8.75)
        recordValue(0.0)
        expectValue(4.375)
      }
    }

    "Handle multiple different instances" in {
      testMultipleStatisticInstances(Seq(
        ProviderTestData(new ExponentiallyWeightedMovingAverage(0.95), recordedValues = Seq(5.0, 5.0), expectedValue = 5.0),
        ProviderTestData(new ExponentiallyWeightedMovingAverage(0.50), recordedValues = Seq(10.0, 5.0), expectedValue = 7.5)))
    }
  }
}
