package org.broadinstitute.monster.dap

import java.io.IOException
import java.time.Duration
import java.time.format.DateTimeFormatter
import okhttp3._
import okhttp3.logging.HttpLoggingInterceptor
import okhttp3.logging.HttpLoggingInterceptor.{Level, Logger}
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
    * @param request ADT capturing the various parameters for a request to a particular endpoint
    */
  def get(
    apiToken: String,
    request: RedcapRequest
  ): Future[Msg]
}

object RedCapClient {
  /** URL for the production RedCap API. */
  private val apiRoute = "https://redcap.dogagingproject.org/api/"

  /** Formatter matching the production RedCap's interface. */
  private val dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

  /** Timeout to use for all requests to production RedCap. */
  private val timeout = Duration.ofSeconds(60)

  /** Construct a client instance backed by the production RedCap instance. */
  def apply(arm: List[String]): RedCapClient = {
    val logger = LoggerFactory.getLogger(getClass)

    val client = new OkHttpClient.Builder()
      .connectTimeout(timeout)
      .readTimeout(timeout)
      .addInterceptor(new HttpLoggingInterceptor(new Logger() {
        override def log(s: String): Unit = {
          logger.info(s)
        }
      }).setLevel(Level.BASIC))
      .build()

    (apiToken, redcapRequest) => {

      val formBuilder = redcapRequest match {
        case GetRecords(ids, fields, forms, start, end, filters) =>
          val logPieces = List(
            s"ids: [${ids.mkString(",")}]",
            s"fields: [${fields.mkString(",")}]",
            s"forms: [${forms.mkString(",")}]",
            s"start: [$start]",
            s"end: [$end]",
            s"filters: [${filters.map(directive => s"${directive.field}${directive.operation.op}${directive.comparand}").mkString(",")}]"
          )
          logger.debug(s"Querying RedCap for records: ${logPieces.mkString(",")}")

          val formBuilder = new FormBody.Builder()
            .add("token", apiToken)
            // Export individual survey records as JSON.
            .add("content", "record")
            .add("format", "json")
            .add("returnFormat", "json")
            .add("type", "eav")
            // Get raw answers so we can pass through data when possible.
            .add("rawOrLabel", "raw")
            // Keep field keys as raw strings, to make programmatic manipulation easier.
            .add("rawOrLabelHeaders", "raw")
            .add("exportCheckboxLabel", "false")
            // Honestly not sure what these do, haven't seen the need to make them 'true'.
            .add("exportSurveyFields", "false")
            .add("exportDataAccessGroups", "false")
          // Parameterized arm
          if (arm.nonEmpty) {
            formBuilder.add("events", arm.mkString(","))
          }

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
          if (filters.nonEmpty) {
            formBuilder.add(
              "filterLogic",
              filters.map { directive =>
                s"[${directive.field}]${directive.operation.op}${directive.comparand}"
              }.mkString(" and ")
            )
          }
          formBuilder
        case GetDataDictionary(instrument) =>
          val logPieces = List(
            s"forms: [${instrument}]"
          )
          logger.debug(s"Querying RedCap for data dictionary: ${logPieces.mkString(",")}")

          new FormBody.Builder()
            .add("token", apiToken)
            // Export individual survey records as JSON.
            .add("content", "metadata")
            .add("format", "json")
            .add("returnFormat", "json")
            .add("forms[0]", instrument)
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
          override def onResponse(call: Call, response: Response): Unit = {
            val responseBody = response.body().string()
            val maybeResult =
              JsonParser.parseEncodedJsonReturningFailure(responseBody)
            maybeResult match {
              case Right(result) => p.success(result)
              case Left(err) => {
                logger.error(s"Error in RedCAP response body: ${responseBody}")
                p.failure(err)
              }
            }
          }
        })
      p.future
    }
  }
}
