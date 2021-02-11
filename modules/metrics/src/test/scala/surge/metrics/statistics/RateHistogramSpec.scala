// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.metrics.statistics

class RateHistogramSpec extends StatisticsSpec {
  "RateHistogram" should {
    "Properly track the minimum observed value" in {
      testForProvider(new RateHistogram(10L)) { context =>
        import context._

        expectValue(0.0)
        recordValues(Seq(1.0, 5.0, 4.0))
        expectValue(1.0)
        recordValue(-11.0)
        expectValue(-0.1)
      }
    }

    "Handle multiple different instances" in {
      testMultipleStatisticInstances(Seq(
        ProviderTestData(new RateHistogram(10L), recordedValues = Seq(5.0, 5.0), expectedValue = 1.0),
        ProviderTestData(new RateHistogram(2L), recordedValues = Seq(3.0, 2.0), expectedValue = 2.5)))
    }
  }
}
