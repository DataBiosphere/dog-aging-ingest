package org.broadinstitute.monster.dap.cslb

import com.spotify.scio.ScioContext
import com.spotify.scio.values.SCollection
import org.broadinstitute.monster.common.{PipelineBuilder, StorageIO}
import org.broadinstitute.monster.common.msg._
import org.broadinstitute.monster.dap.common.{Args, RawRecord}
import org.slf4j.{Logger, LoggerFactory}

object CslbTransformationPipelineBuilder extends PipelineBuilder[Args] {
  /**
    * Schedule all the steps for the Dog Aging transformation in the given pipeline context.
    *
    * Scheduled steps are launched against the context's runner when the `run()` method
    * is called on it.
    *
    * Adding implicit logger so we can associate it with the PipelineBuilder object
    */
  implicit val logger: Logger = LoggerFactory.getLogger(getClass)

  override def buildPipeline(ctx: ScioContext, args: Args): Unit = {
    val rawRecords = readRecords(ctx, args)

    val cslbTransformations =
      rawRecords.transform("CSLB data")(_.flatMap(CslbTransformations.mapCslbData))

    StorageIO.writeJsonLists(
      cslbTransformations,
      "CSLB data",
      s"${args.outputPrefix}/cslb"
    )
    ()
  }

  /** Read in records and group by study Id, with field name subgroups. */
  def readRecords(ctx: ScioContext, args: Args): SCollection[RawRecord] = {
    val rawRecords = StorageIO
      .readJsonLists(
        ctx,
        "Raw Records",
        s"${args.inputPrefix}/records/*.json"
      )

    // Group by study ID (record number) and arm (address_year_month)
    // to get the format: (studyId, arm_id, Iterable((fieldName, Iterable(value))))
    rawRecords
      .groupBy(record => {
        (record.read[String]("record"), record.read[String]("redcap_event_name"))
      })
      .map {
        case ((id, eventName), rawRecordValues) =>
          val fields = rawRecordValues
            .groupBy(_.read[String]("field_name"))
            .map {
              case (fieldName, rawValues) =>
                (fieldName, rawValues.map(_.read[String]("value")).toArray.sorted)
            }
          RawRecord(id.toLong, fields + ("redcap_event_name" -> Array(eventName)))
      }
  }
}
