// Copyright © 2017-2021 UKG Inc. <https://www.ukg.com>

package surge.metrics.statistics

class CountSpec extends StatisticsSpec {
  "Count" should {
    "Properly track the count of something" in {
      testForProvider(new Count) { context =>
        import context._

        expectValue(0.0)
        recordValues(Seq(1.0, 5.0, 4.0))
        expectValue(10.0)
        recordValue(-11.0)
        expectValue(-1.0)
      }
    }

    "Handle multiple different instances" in {
      testMultipleStatisticInstances(
        Seq(
          ProviderTestData(new Count, recordedValues = Seq(5.0, 5.0), expectedValue = 10.0),
          ProviderTestData(new Count, recordedValues = Seq(3.0, 2.0), expectedValue = 5.0)))
    }
  }
}
