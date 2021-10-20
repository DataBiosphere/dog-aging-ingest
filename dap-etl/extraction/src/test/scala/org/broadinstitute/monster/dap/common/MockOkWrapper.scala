package org.broadinstitute.monster.dap.common

import okhttp3._
import upack.Msg

import scala.concurrent.{Future, Promise}

class MockOkWrapper(
  responseMap: Map[RedcapRequestGeneratorParams, Msg] = Map.empty
) extends HttpWrapper {

  // Requests use the default Object.equals() implementation,
  // so to match them on content we need to use this custom method.
  // This method isn't exhaustive - it ignores distinctions between http/https
  // and any differences in headers, but it's good enough for our tests as-is.
  def requestsMatch(firstRequest: Request, secondRequest: Request): Boolean = {
    firstRequest.url.equals(secondRequest.url) &&
    firstRequest.method.equals(secondRequest.method) &&
    formBodiesMatch(
      firstRequest.body.asInstanceOf[FormBody],
      secondRequest.body.asInstanceOf[FormBody]
    )
  }

  // Checks that form bodies have the same number of values and that the values are identical.
  def formBodiesMatch(firstBody: FormBody, secondBody: FormBody): Boolean = {
    // the ordering of records in a request is non-deterministic, so we convert the non-record fields to a map
    // then extract the record IDs to sets and compare
    val firstMap: Map[String, String] = Range(0, firstBody.size)
      .filter(index => !firstBody.name(index).contains("record"))
      .map(index => {
        secondBody.name(index) -> secondBody.value(index)
      })
      .toMap

    val secondMap: Map[String, String] = Range(0, secondBody.size)
      .filter(index => !secondBody.name(index).contains("record"))
      .map(index => {
        secondBody.name(index) -> secondBody.value(index)
      })
      .toMap

    val firstRecordsSet: Set[String] = Range(0, firstBody.size)
      .filter(index => firstBody.name(index).contains("record"))
      .map(index => {
        firstBody.value(index)
      })
      .toSet

    val secondRecordsSet: Set[String] = Range(0, secondBody.size)
      .filter(index => secondBody.name(index).contains("record"))
      .map(index => {
        secondBody.value(index)
      })
      .toSet

    firstBody.size == secondBody.size &&
    firstMap == secondMap &&
    firstRecordsSet == secondRecordsSet
  }

  def makeRequest(request: Request): Future[Msg] = {
    val p = Promise[Msg]()

    val mapping = responseMap.map {
      case (requestParams, mappedResponse) =>
        RedCapClient.buildRequest(requestParams) -> mappedResponse
    }

    val matchingRequest = mapping.keys.find(requestKey => requestsMatch(requestKey, request))

    matchingRequest match {
      case Some(matchedRequest) => p.success(mapping(matchedRequest))
      case None                 => p.failure(new RuntimeException("No mock response defined for request"))
    }

    p.future
  }
}
