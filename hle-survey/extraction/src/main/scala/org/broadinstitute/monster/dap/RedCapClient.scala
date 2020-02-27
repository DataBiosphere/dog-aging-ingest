package org.broadinstitute.monster.dap

import java.io.IOException
import java.time.OffsetDateTime
import java.time.format.DateTimeFormatter

import okhttp3.{Call, Callback, FormBody, OkHttpClient, Request, Response}
import org.broadinstitute.monster.common.msg.JsonParser
import upack.Msg

import scala.concurrent.{Future, Promise}

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

  def apply(): RedCapClient = {
    val client = new OkHttpClient()

    (apiToken, ids, fields, forms, start, end, valueFilters) => {
      val formBuilder = new FormBody.Builder()
        .add("token", apiToken)
        .add("content", "record")
        .add("format", "json")
        .add("returnFormat", "json")
        .add("type", "flat")
        .add("rawOrLabel", "label")
        .add("rawOrLabelHeaders", "raw")
        .add("exportCheckboxLabel", "false")
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
