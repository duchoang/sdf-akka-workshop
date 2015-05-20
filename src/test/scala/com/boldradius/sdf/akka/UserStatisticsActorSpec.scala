package com.boldradius.sdf.akka

import akka.testkit.TestActorRef
import com.boldradius.sdf.akka.UserStatisticsActor.Percent
import org.joda.time.DateTime

class UserStatisticsActorSpec extends BaseAkkaSpec {

  val userStatsRef = TestActorRef(new UserStatisticsActor)

  "Aggregation functions" should {
    "Count number of requests by browser" in {
      val result = userStatsRef.underlyingActor.browserUsersAggregation(
        List(testChromeRequest, testChromeRequest, testChromeRequest, testFirefoxRequest, testIERequest)
      )
      result shouldEqual Map("Chrome" -> 3, "Firefox" -> 1, "IE10" -> 1)
    }

    "Count percentage of requests by page" in {
      val userStatsRef = TestActorRef(new UserStatisticsActor)
      val requests = List(testChromeRequest, testChromeRequest, testChromeRequest, testOtherPageRequest, testOtherPageRequest)
      val result = userStatsRef.underlyingActor.pageVisitsAggregation(requests)
      result shouldEqual Map(url1 -> Percent(60), url3 -> Percent(40))
      val result2 = userStatsRef.underlyingActor.pageVisitsAggregation(testOtherPageRequest :: requests)
      result2 shouldEqual Map(url1 -> Percent(50), url3 -> Percent(50))
    }

    "Count number of requests per minute" in {
      val result = userStatsRef.underlyingActor.timeAggregation(
        List(testChromeRequest, testChromeRequest, testChromeRequest, testFirefoxRequest, testIERequest)
      )
      result shouldEqual Map((14,20) -> 3, (14, 30) -> 1, (14, 35) -> 1)
    }

    "Count total visit time per url" in {
      val result = userStatsRef.underlyingActor.visitTimePerURLAggregation(
        List(testChromeRequest, testFirefoxRequest, testIERequest)
      )
      result shouldEqual Map("/home" -> 600000, "/contact" -> 300000)
    }
  }

  val time1 = new DateTime(2015, 5, 20, 14, 20)
  val time2 = new DateTime(2015, 5, 20, 14, 30)
  val time3 = new DateTime(2015, 5, 20, 14, 35)
  val url1 = "/home"
  val url2 = "/contact"
  val url3 = "/product"

  val testChromeRequest = Request(42l, time1.getMillis, url1, "", "Chrome")
  val testFirefoxRequest = testChromeRequest.copy(browser = "Firefox", timestamp = time2.getMillis, url = url2)
  val testIERequest = testChromeRequest.copy(browser = "IE10", timestamp = time3.getMillis, url = url1)
  val testOtherPageRequest = testChromeRequest.copy(url = url3)
}
