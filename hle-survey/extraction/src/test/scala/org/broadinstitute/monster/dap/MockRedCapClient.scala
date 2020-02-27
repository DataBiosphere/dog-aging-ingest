package org.broadinstitute.monster.dap
import java.time.OffsetDateTime

import upack.Msg

import scala.concurrent.Future
import scala.collection.mutable

/** Mock implementation of the RedCap client, for unit testing. */
class MockRedCapClient(
  expectedToken: String,
  responseMap: Map[MockRedCapClient.QueryParams, Msg]
) extends RedCapClient {
  val recordedRequests = mutable.Set[MockRedCapClient.QueryParams]()

  override def getRecords(
    apiToken: String,
    ids: List[String],
    fields: List[String],
    forms: List[String],
    start: Option[OffsetDateTime],
    end: Option[OffsetDateTime],
    valueFilters: Map[String, String]
  ): Future[Msg] = {
    if (apiToken != expectedToken) {
      Future.failed(new RuntimeException(s"Mysterious token: $apiToken"))
    }

    val params = MockRedCapClient.QueryParams(
      ids = ids,
      fields = fields,
      forms = forms,
      start = start,
      end = end,
      filters = valueFilters
    )
    recordedRequests.add(params)
    responseMap
      .get(params)
      .fold(Future.failed[Msg](new RuntimeException("404")))(Future.successful)
  }
}

object MockRedCapClient {

  /** Utility class used to bundle query params together, for easy set / map lookup. */
  case class QueryParams(
    ids: List[String] = Nil,
    fields: List[String] = Nil,
    forms: List[String] = Nil,
    start: Option[OffsetDateTime] = None,
    end: Option[OffsetDateTime] = None,
    filters: Map[String, String] = Map.empty
  )
}
