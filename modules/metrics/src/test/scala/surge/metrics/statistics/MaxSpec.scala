// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.metrics.statistics

class MaxSpec extends StatisticsSpec {
  "Max" should {
    "Properly track the maximum observed value" in {
      testForProvider(new Max) { context =>
        import context._

        expectValue(0.0)
        recordValues(Seq(1.0, 5.0, 4.0))
        expectValue(5.0)
        recordValue(-11.0)
        expectValue(5.0)
      }
    }

    "Handle multiple different instances" in {
      testMultipleStatisticInstances(
        Seq(
          ProviderTestData(new Max, recordedValues = Seq(5.0, 5.0), expectedValue = 5.0),
          ProviderTestData(new Max, recordedValues = Seq(3.0, 2.0), expectedValue = 3.0)))
    }
  }
}
