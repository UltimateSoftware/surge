// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.metrics.statistics

class MinSpec extends StatisticsSpec {
  "Min" should {
    "Properly track the minimum observed value" in {
      testForProvider(new Min) { context =>
        import context._

        expectValue(0.0)
        recordValues(Seq(1.0, 5.0, 4.0))
        expectValue(1.0)
        recordValue(-11.0)
        expectValue(-11.0)
      }
    }

    "Handle multiple different instances" in {
      testMultipleStatisticInstances(
        Seq(
          ProviderTestData(new Min, recordedValues = Seq(5.0, 5.0), expectedValue = 5.0),
          ProviderTestData(new Min, recordedValues = Seq(3.0, 2.0), expectedValue = 2.0)))
    }
  }
}
