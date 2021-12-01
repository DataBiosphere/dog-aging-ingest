package org.broadinstitute.monster.dap.common

import okhttp3.Request
import okhttp3.mockwebserver.{MockResponse, MockWebServer}
import org.broadinstitute.monster.dap.common.OkWrapper.MAX_RETRIES
import org.scalatest.flatspec.FixtureAsyncFlatSpec
import upack.Int64

import java.io.IOException

class OkWrapperSpec extends FixtureAsyncFlatSpec {

  behavior of "OkWrapper"

  type FixtureParam = MockWebServer

  override def withFixture(test: OneArgAsyncTest) = {
    val server = new MockWebServer
    server.start()

    complete {
      withFixture(test.toNoArgAsyncTest(server))
    } lastly {
      server.shutdown()
    }
  }

  it should "fail out after MAX_RETRIES" in { server =>
    val serverUrl = server.url("/v1/testing")
    val okWrapper = new OkWrapper
    val request = new Request.Builder().url(serverUrl).build()
    (0 to MAX_RETRIES).foreach { _ =>
      server.enqueue(new MockResponse().setResponseCode(500).setBody("error"))
    }

    recoverToSucceededIf[IOException] {
      okWrapper.makeRequest(request)
    }
  }

  it should "retry and succeed after a 500 response" in { server =>
    val serverUrl = server.url("/v1/testing")
    val okWrapper = new OkWrapper
    val request = new Request.Builder().url(serverUrl).build()
    server.enqueue(new MockResponse().setResponseCode(500).setBody("error"))
    server.enqueue(new MockResponse().setResponseCode(200).setBody("[1]"))

    okWrapper.makeRequest(request).map { result =>
      assert(result.arr.length == 1)
      assert(result.arr(0) == Int64(1))
    }
  }

}
