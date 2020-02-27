package org.broadinstitute.monster.dap

import java.io.IOException
import java.time.{Duration, OffsetDateTime}
import java.time.format.DateTimeFormatter

import okhttp3.{Call, Callback, FormBody, OkHttpClient, Request, Response}
import org.broadinstitute.monster.common.msg.JsonParser
import org.slf4j.LoggerFactory
import upack.Msg

import scala.concurrent.{Future, Promise}

/** TODO */
trait RedCapClient extends Serializable {

  def getRecords(
    apiToken: String,
    ids: Set[String] = Set.empty,
    fields: Set[String] = Set.empty,
    forms: Set[String] = Set.empty,
    start: Option[OffsetDateTime] = None,
    end: Option[OffsetDateTime] = None,
    valueFilters: Map[String, String] = Map.empty
  ): Future[Msg]
}

object RedCapClient {
  private val apiRoute = "https://cdsweb07.fhcrc.org/redcap/api/"
  private val dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

  /** TODO */
  def apply(): RedCapClient = {
    val logger = LoggerFactory.getLogger(getClass)

    val client = new OkHttpClient.Builder()
      .connectTimeout(Duration.ofSeconds(60))
      .readTimeout(Duration.ofSeconds(60))
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
      start.foreach { s =>
        formBuilder.add("dateRangeBegin", s.format(dateFormatter))
      }
      end.foreach { e =>
        formBuilder.add("dateRangeEnd", e.format(dateFormatter))
      }
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
