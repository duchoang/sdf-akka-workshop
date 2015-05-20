package com.boldradius.sdf.akka

import akka.testkit.TestActorRef
import com.boldradius.sdf.akka.UserStatisticsActor.Percent

class UserStatisticsActorSpec extends BaseAkkaSpec {

  "Aggregation functions" should {
    "Count number of requests by browser" in {
      val userStatsRef = TestActorRef(new UserStatisticsActor)
      val result = userStatsRef.underlyingActor.browserUsersAggregation(
        List(testChromeRequest, testChromeRequest, testChromeRequest, testFirefoxRequest, testIERequest)
      )
      result shouldEqual Map("Chrome" -> 3, "Firefox" -> 1, "IE10" -> 1)
    }

    "Count percentage of requests by page" in {
      val userStatsRef = TestActorRef(new UserStatisticsActor)
      val requests = List(testChromeRequest, testChromeRequest, testChromeRequest, testOtherPageRequest, testOtherPageRequest)
      val result = userStatsRef.underlyingActor.pageVisitsAggregation(requests)
      result shouldEqual Map("test.org" -> Percent(60), "test.org/index.html" -> Percent(40))
      val result2 = userStatsRef.underlyingActor.pageVisitsAggregation(testOtherPageRequest :: requests)
      result2 shouldEqual Map("test.org" -> Percent(50), "test.org/index.html" -> Percent(50))
    }
  }

  val testChromeRequest = Request(42l, 42l, "test.org", "", "Chrome")
  val testFirefoxRequest = testChromeRequest.copy(browser = "Firefox")
  val testIERequest = testChromeRequest.copy(browser = "IE10")
  val testOtherPageRequest = testChromeRequest.copy(url = "test.org/index.html")
}
