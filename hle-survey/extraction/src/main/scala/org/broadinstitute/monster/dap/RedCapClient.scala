package org.broadinstitute.monster.dap

import java.io.IOException
import java.time.{Duration, OffsetDateTime}
import java.time.format.DateTimeFormatter

import okhttp3.{Call, Callback, FormBody, OkHttpClient, Request, Response}
import org.broadinstitute.monster.common.msg.JsonParser
import org.slf4j.LoggerFactory
import upack.Msg

import scala.concurrent.{Future, Promise}

/** Interface for clients that can pull records from a RedCap API. */
trait RedCapClient extends Serializable {

  /**
    * Download survey responses from the RedCap API backing this client.
    *
    * NOTE: Record-level filters are combined with AND-ing logic. Field-level
    * filters are combined with OR-ing logic.
    *
    * @param apiToken auth token to use when querying the API
    * @param ids IDs of the specific records to download. If not set, all
    *            records will be downloaded
    * @param fields subset of fields to download. If not set, all fields
    *               will be downloaded
    * @param forms subset of forms to download. If not set, fields from all
    *              forms will be downloaded
    * @param start if given, only records created-or-updated at or after this
    *              time will be downloaded
    * @param end if given, only records created-or-updated before or at this
    *            time will be downloaded
    * @param valueFilters arbitrary field-value pairs to use as an exact-match
    *                     filter on downloaded records
    */
  def getRecords(
    apiToken: String,
    ids: List[String] = Nil,
    fields: List[String] = Nil,
    forms: List[String] = Nil,
    start: Option[OffsetDateTime] = None,
    end: Option[OffsetDateTime] = None,
    valueFilters: Map[String, String] = Map.empty
  ): Future[Msg]
}

object RedCapClient {
  /** URL for the production RedCap API. */
  private val apiRoute = "http://redcap.fredhutch.org"

  /** Formatter matching the production RedCap's interface. */
  private val dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

  /** Timeout to use for all requests to production RedCap. */
  private val timeout = Duration.ofSeconds(60)

  /** Construct a client instance backed by the production RedCap instance. */
  def apply(): RedCapClient = {
    val logger = LoggerFactory.getLogger(getClass)

    val client = new OkHttpClient.Builder()
      .connectTimeout(timeout)
      .readTimeout(timeout)
      .build()

    (apiToken, ids, fields, forms, start, end, valueFilters) => {
      val logPieces = List(
        s"ids: [${ids.mkString(",")}]",
        s"fields: [${fields.mkString(",")}]",
        s"forms: [${forms.mkString(",")}]",
        s"start: [$start]",
        s"end: [$end]",
        s"filters: [${valueFilters.map { case (k, v) => s"$k=$v" }.mkString(",")}]"
      )
      logger.debug(s"Querying RedCap: ${logPieces.mkString(",")}")

      val formBuilder = new FormBody.Builder()
        .add("token", apiToken)
        // Limit to the initial HLE event.
        .add("events[0]", "baseline_arm_1")
        // Export individual survey records as JSON.
        .add("content", "record")
        .add("format", "json")
        .add("returnFormat", "json")
        .add("type", "flat")
        // Get labeled answers so we can pass through data when possible.
        .add("rawOrLabel", "label")
        // Keep field keys as raw strings, to make programmatic manipulation easier.
        .add("rawOrLabelHeaders", "raw")
        .add("exportCheckboxLabel", "false")
        // Honestly not sure what these do, haven't seen the need to make them 'true'.
        .add("exportSurveyFields", "false")
        .add("exportDataAccessGroups", "false")

      ids.zipWithIndex.foreach {
        case (id, i) => formBuilder.add(s"records[$i]", id)
      }
      fields.zipWithIndex.foreach {
        case (f, i) => formBuilder.add(s"fields[$i]", f)
      }
      forms.zipWithIndex.foreach {
        case (f, i) => formBuilder.add(s"forms[$i]", f)
      }
      start.foreach(s => formBuilder.add("dateRangeBegin", s.format(dateFormatter)))
      end.foreach(e => formBuilder.add("dateRangeEnd", e.format(dateFormatter)))
      valueFilters.foreach {
        case (k, v) => formBuilder.add("filterLogic", s"[$k]=$v")
      }

      val request = new Request.Builder()
        .url(apiRoute)
        .post(formBuilder.build())
        .build()

      val p = Promise[Msg]()
      client
        .newCall(request)
        .enqueue(new Callback {
          override def onFailure(call: Call, e: IOException): Unit =
            p.failure(e)
          override def onResponse(call: Call, response: Response): Unit =
            p.success(JsonParser.parseEncodedJson(response.body().string()))
        })
      p.future
    }
  }
}
