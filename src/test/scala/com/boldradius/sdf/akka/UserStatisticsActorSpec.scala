package com.boldradius.sdf.akka

import akka.testkit.TestActorRef

class UserStatisticsActorSpec extends BaseAkkaSpec {

  "Aggregation functions" should {
    "Count number of requests by browser" in {
      val userStatsRef = TestActorRef(new UserStatisticsActor)
      val result = userStatsRef.underlyingActor.browserUsersAggregation(
        List(testChromeRequest, testChromeRequest, testChromeRequest, testFirefoxRequest, testIERequest)
      )
      result shouldEqual Map("Chrome" -> 3, "Firefox" -> 1, "IE10" -> 1)
    }
  }

  val testChromeRequest = Request(42l, 42l, "test.org", "", "Chrome")
  val testFirefoxRequest = testChromeRequest.copy(browser = "Firefox")
  val testIERequest = testChromeRequest.copy(browser = "IE10")
}
