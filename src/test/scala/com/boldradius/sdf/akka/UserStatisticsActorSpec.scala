package com.boldradius.sdf.akka

import akka.testkit.TestActorRef
import com.boldradius.sdf.akka.UserStatisticsActor.Percent
import org.joda.time.DateTime

class UserStatisticsActorSpec extends BaseAkkaSpec {

  "Aggregation functions" should {
    "Count number of requests by browser" in {
      val requests = List(testChromeRequest, testChromeRequest, testChromeRequest, testFirefoxRequest, testIERequest)
      val result = UserStatisticsActor.all(requests, UserStatisticsActor.groupByBrowser, UserStatisticsActor.mapToCount)
      result shouldEqual Map("Chrome" -> 3, "Firefox" -> 1, "IE10" -> 1)
    }

    "Count percentage of requests by page" in {
      val requests = List(testChromeRequest, testChromeRequest, testChromeRequest, testOtherPageRequest, testOtherPageRequest)
      val result = UserStatisticsActor.all(requests, UserStatisticsActor.groupByUrl, UserStatisticsActor.mapToCount)
      val newRes = result.mapValues(size => UserStatisticsActor.sizeToPercent(size, requests.size))
      newRes shouldEqual Map(url1 -> Percent(60), url3 -> Percent(40))
      val result2 = UserStatisticsActor.all(testOtherPageRequest :: requests, UserStatisticsActor.groupByUrl, UserStatisticsActor.mapToCount)
      val newRes2 = result2.mapValues(size => UserStatisticsActor.sizeToPercent(size, requests.size + 1))
      newRes2 shouldEqual Map(url1 -> Percent(50), url3 -> Percent(50))
    }

    "Provide top three landing pages and hits" in {
      val requests = List(testChromeRequest, testChromeRequest, testChromeRequest, testOtherPageRequest, testOtherPageRequest,
        testLandingPage1, testLandingPage1, testLandingPage2)
      val result = UserStatisticsActor.top(3, requests, UserStatisticsActor.groupByUrl, UserStatisticsActor.mapToCount)
      result shouldEqual Map(url1 -> 3, url3 -> 2, url4 -> 2)
    }

    "Count number of requests per minute" in {
      val requests = List(testChromeRequest, testChromeRequest, testChromeRequest, testFirefoxRequest, testIERequest)
      val result = UserStatisticsActor.all(requests, UserStatisticsActor.groupByTime, UserStatisticsActor.mapToCountByTime)
      result shouldEqual Map((14,20) -> 3, (14, 30) -> 1, (14, 35) -> 1)
    }

    "Count total visit time per url" in {
      val result = UserStatisticsActor.visitTimePerPage(
        List(testChromeRequest, testFirefoxRequest, testIERequest)
      )
      result shouldEqual Map("/home" -> 600000, "/contact" -> 300000)
    }

    "Get top browsers" in {
      val requests = List(testChromeRequest, testChromeRequest, testChromeRequest, testFirefoxRequest, testFirefoxRequest2, testIERequest, testChromeRequest2)
      val result = UserStatisticsActor.top(2, requests, UserStatisticsActor.groupByBrowser, UserStatisticsActor.mapToUserCount)
      result shouldEqual Map("Chrome" -> 2, "Firefox" -> 2)
    }

    "Get top referrers" in {
      val requests = List(testChromeRequest, testFirefoxRequest, testIERequest, testChromeRequest2, testFirefoxRequest2, testChromeRequest3)
      val result = UserStatisticsActor.top(2, requests, UserStatisticsActor.groupByReferrer, UserStatisticsActor.mapToUserCount)
      result shouldEqual Map("google" -> 3, "facebook" -> 2)
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
