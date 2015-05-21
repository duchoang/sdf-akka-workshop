package com.boldradius.sdf.akka

import akka.testkit.TestActorRef
import com.boldradius.sdf.akka.UserStatisticsActor.Percent
import org.joda.time.DateTime

class UserStatisticsActorSpec extends BaseAkkaSpec {

  val userStatsRef = TestActorRef(new UserStatisticsActor)

  "Aggregation functions" should {
    "Count number of requests by browser" in {
      userStatsRef.underlyingActor.browserAggregation(
        List(testChromeRequest, testChromeRequest, testChromeRequest, testFirefoxRequest, testIERequest)
      )
      val result = userStatsRef.underlyingActor.requestsPerBrowserAggregation
      result shouldEqual Map("Chrome" -> 3, "Firefox" -> 1, "IE10" -> 1)
    }

    "Count percentage of requests by page" in {
      val requests = List(testChromeRequest, testChromeRequest, testChromeRequest, testOtherPageRequest, testOtherPageRequest)
      val result = userStatsRef.underlyingActor.pageVisitsAggregation(requests)
      result shouldEqual Map(url1 -> Percent(60), url3 -> Percent(40))
      val result2 = userStatsRef.underlyingActor.pageVisitsAggregation(testOtherPageRequest :: requests)
      result2 shouldEqual Map(url1 -> Percent(50), url3 -> Percent(50))
    }

    "Provide top three landing pages and hits" in {
      val requests = List(testChromeRequest, testChromeRequest, testChromeRequest, testOtherPageRequest, testOtherPageRequest,
        testLandingPage1, testLandingPage1, testLandingPage2)
      val result = userStatsRef.underlyingActor.top(3, requests, UserStatisticsActor.groupByUrl, UserStatisticsActor.mapToCount)
      result shouldEqual Map(url1 -> 3, url3 -> 2, url4 -> 2)
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

    "Get top browsers new" in {
      val userStatsRef2 = TestActorRef(new UserStatisticsActor)
      val requests = List(testChromeRequest, testChromeRequest, testChromeRequest, testFirefoxRequest, testFirefoxRequest2, testIERequest, testChromeRequest2)
      val result = userStatsRef2.underlyingActor.top(2,requests, UserStatisticsActor.groupByBrowser, UserStatisticsActor.mapToUserCount)
      result shouldEqual Map("Chrome" -> 2, "Firefox" -> 2)
    }

    "Get top browsers" in {
      val userStatsRef2 = TestActorRef(new UserStatisticsActor)
      userStatsRef2.underlyingActor.browserAggregation(
        List(testChromeRequest, testChromeRequest, testChromeRequest, testFirefoxRequest, testIERequest, testChromeRequest2)
      )
      val result: Map[String, Int] = userStatsRef2.underlyingActor.usersPerBrowserAggregation
      result shouldEqual Map("IE10" -> 1, "Chrome" -> 2, "Firefox" -> 1)

      val top = userStatsRef2.underlyingActor.getTopBrowser(2)
      top shouldEqual List(("Chrome", 2), ("IE10", 1))
    }

    "Get top referrer" in {
      val userStatsRef3 = TestActorRef(new UserStatisticsActor)
      userStatsRef3.underlyingActor.referrerAggregation(
        List(testChromeRequest, testFirefoxRequest, testIERequest, testChromeRequest2, testFirefoxRequest2, testChromeRequest3)
      )
      val result = userStatsRef3.underlyingActor.getTopReferrer(3)
      result shouldEqual List(("google", 3), ("facebook", 2), ("microsoft", 1))

    }
  }

  val time1 = new DateTime(2015, 5, 20, 14, 20)
  val time2 = new DateTime(2015, 5, 20, 14, 30)
  val time3 = new DateTime(2015, 5, 20, 14, 35)
  val url1 = "/home"
  val url2 = "/contact"
  val url3 = "/product"
  val url4 = "/about"
  val user1 = 42l
  val user2 = 10l
  val user3 = 12l

  val testChromeRequest = Request(user1, time1.getMillis, url1, "google", "Chrome")
  val testFirefoxRequest = testChromeRequest.copy(browser = "Firefox", timestamp = time2.getMillis, url = url2, referrer = "facebook")
  val testIERequest = testChromeRequest.copy(browser = "IE10", timestamp = time3.getMillis, url = url1, referrer = "microsoft")
  val testOtherPageRequest = testChromeRequest.copy(url = url3)
  val testChromeRequest2 = testChromeRequest.copy(sessionId = user2)
  val testChromeRequest3 = testChromeRequest.copy(sessionId = user3)
  val testFirefoxRequest2 = testFirefoxRequest.copy(sessionId = user3)

  val testLandingPage1 = testChromeRequest.copy(url = url4)
  val testLandingPage2 = testChromeRequest.copy(url = "test.org/about.html")

}
