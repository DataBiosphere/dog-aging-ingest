package org.broadinstitute.monster.dap

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import okhttp3.FormBody
import org.broadinstitute.monster.dap.common.{FilterDirective, FilterOps}

class RedCapClientSpec extends AnyFlatSpec with Matchers {

  def getRequestBodyFields(body: FormBody): Map[String, String] = {
    (0 until body.size).map(index => Map(body.name(index) -> body.value(index))).reduce {
      (map, pair) =>
        map ++ pair
    }
  }

  behavior of "RedCapClient"

  it should "properly escape spaces in its generated filter logic" in {
    val params = RedcapRequestGeneratorParams(
      "fakepitoken",
      List("some_arm"),
      GetRecords(
        filters = List(
          FilterDirective("unspaced", FilterOps.>, "4"),
          FilterDirective("spaced", FilterOps.==, "steve was here")
        )
      )
    )

    val generatedRequest = RedCapClient.buildRequest(params)
    val bodyFields = getRequestBodyFields(generatedRequest.body.asInstanceOf[FormBody])
    bodyFields("filterLogic") shouldBe "[unspaced]>4 and [spaced]=\"steve was here\""
  }
}
