package org.broadinstitute.monster.dap

import java.time.OffsetDateTime
import java.time.format.DateTimeFormatter

import okhttp3._
import org.slf4j.LoggerFactory
import upack.Msg

import scala.concurrent.Future

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

case class RedcapRequestGeneratorParams(
  apiToken: String,
  arm: List[String],
  redcapRequest: RedcapRequest
)

object RedCapClient {
  /** URL for the production RedCap API. */
  private val apiRoute = "https://redcap.dogagingproject.org/api/"

  def redcapFormatDate(date: OffsetDateTime): String =
    date.format(DateTimeFormatter.ofPattern("yyyy-MM-dd hh:mm"))

  def buildRequest(generatorParams: RedcapRequestGeneratorParams): Request = {
    val logger = LoggerFactory.getLogger(getClass)

    val formBuilder = generatorParams.redcapRequest match {
      case GetRecords(ids, fields, forms, filters, arm) =>
        val logPieces = List(
          s"ids: [${ids.mkString(",")}]",
          s"fields: [${fields.mkString(",")}]",
          s"forms: [${forms.mkString(",")}]",
          s"filters: [${filters
            .map(directive => s"${directive.field}${directive.operation.op}${directive.spaceEscapedComparand}")
            .mkString(",")}]",
          s"arm: [$arm]"
        )
        logger.debug(s"Querying RedCap for records: ${logPieces.mkString(",")}")

        val formBuilder = new FormBody.Builder()
          .add("token", generatorParams.apiToken)
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
        if (generatorParams.arm.nonEmpty) {
          formBuilder.add("events", generatorParams.arm.mkString(","))
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
        if (filters.nonEmpty) {
          formBuilder.add(
            "filterLogic",
            filters.map { directive =>
              s"[${directive.field}]${directive.operation.op}${directive.spaceEscapedComparand}"
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
          .add("token", generatorParams.apiToken)
          // Export individual survey records as JSON.
          .add("content", "metadata")
          .add("format", "json")
          .add("returnFormat", "json")
          .add("forms[0]", instrument)
    }

    new Request.Builder()
      .url(apiRoute)
      .post(formBuilder.build())
      .build()
  }

  def apply(arm: List[String], client: HttpWrapper): RedCapClient = { (apiToken, redcapRequest) =>
    {
      val request = buildRequest(RedcapRequestGeneratorParams(apiToken, arm, redcapRequest))
      client.makeRequest(request)
    }
  }
}
