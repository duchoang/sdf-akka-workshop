package com.boldradius.sdf.akka

import akka.testkit.TestActorRef
import org.joda.time.DateTime

class UserStatisticsActorSpec extends BaseAkkaSpec {

  "Aggregation functions" should {
    "Count number of requests by browser" in {
      val result = userStatsRef.underlyingActor.browserUsersAggregation(
        List(testChromeRequest, testChromeRequest, testChromeRequest, testFirefoxRequest, testIERequest)
      )
      result shouldEqual Map("Chrome" -> 3, "Firefox" -> 1, "IE10" -> 1)
    }

    "Count number of requests per minute" in {
      val result = userStatsRef.underlyingActor.timeAggregation(
        List(testChromeRequest, testChromeRequest, testChromeRequest, testFirefoxRequest, testIERequest)
      )
      result shouldEqual Map((14,20) -> 3, (14, 30) -> 2)
    }
  }

  val time1 = new DateTime(2015, 5, 20, 14, 20)
  val time2 = new DateTime(2015, 5, 20, 14, 30)
  val time3 = new DateTime(2015, 5, 20, 14, 30)

  val userStatsRef = TestActorRef(new UserStatisticsActor)
  val testChromeRequest = Request(42l, time1.getMillis, "test.org", "", "Chrome")
  val testFirefoxRequest = testChromeRequest.copy(browser = "Firefox", timestamp = time2.getMillis)
  val testIERequest = testChromeRequest.copy(browser = "IE10", timestamp = time3.getMillis)
}
