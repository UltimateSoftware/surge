// Copyright © 2017-2020 UKG Inc. <https://www.ukg.com>

package surge.metrics.statistics

import java.time.Instant

class MostRecentValueSpec extends StatisticsSpec {
  "MostRecentValue" should {
    "Properly track the latest observed value" in {
      testForProvider(new MostRecentValue) { context =>
        import context._

        expectValue(0.0)
        recordValues(Seq(1.0, 5.0, 4.0))
        expectValue(4.0)
        recordValue(-11.0)
        expectValue(-11.0)
        recordValue(15.0, Instant.now.minusSeconds(3L).toEpochMilli)
      }
    }

    "Handle multiple different instances" in {
      testMultipleStatisticInstances(Seq(
        ProviderTestData(new MostRecentValue, recordedValues = Seq(5.0, 5.0), expectedValue = 5.0),
        ProviderTestData(new MostRecentValue, recordedValues = Seq(3.0, 2.0, 1.0), expectedValue = 1.0)))
    }
  }
}
