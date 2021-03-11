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
  // This functions assumes that form fields are ordered deterministically, which may not be true.
  def formBodiesMatch(firstBody: FormBody, secondBody: FormBody): Boolean = {
    firstBody.size == secondBody.size &&
    Range(0, firstBody.size).forall(index => {
      firstBody.name(index).equals(secondBody.name(index)) &&
        firstBody.value(index).equals(secondBody.value(index))
    })
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
