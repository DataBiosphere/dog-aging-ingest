package org.broadinstitute.monster.dap

import upack.Msg

import scala.concurrent.Future
import scala.collection.mutable

/** Mock implementation of the RedCap client, for unit testing. */
class MockRedCapClient(
  expectedToken: String,
  responseMap: Map[RedcapRequest, Msg]
) extends RedCapClient {
  val recordedRequests = mutable.Set[RedcapRequest]()

  override def get(
    apiToken: String,
    request: RedcapRequest
  ): Future[Msg] = {
    if (apiToken != expectedToken) {
      Future.failed(new RuntimeException(s"Mysterious token: $apiToken"))
    }

    recordedRequests.add(request)
    responseMap
      .get(request)
      .fold(Future.failed[Msg](new RuntimeException("404")))(Future.successful)
  }
}
