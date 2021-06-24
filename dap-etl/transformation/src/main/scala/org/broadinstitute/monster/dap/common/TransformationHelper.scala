package org.broadinstitute.monster.dap.common

import com.spotify.scio.ScioContext
import com.spotify.scio.values.SCollection
import org.broadinstitute.monster.common.{PipelineCoders, StorageIO}
import org.broadinstitute.monster.common.msg._
import upack.Msg

object TransformationHelper extends PipelineCoders {

  def readRecordsGroupByEventName(ctx: ScioContext, inputPrefix: String): SCollection[RawRecord] = {
    val rawRecords = readJsonLists(ctx, inputPrefix)
    val groupByPredicate = { record: Msg =>
      (record.read[String]("record"), record.read[String]("redcap_event_name"))
    }

    rawRecords
      .groupBy(groupByPredicate)
      .map {
        case ((id: String, eventName), rawRecordValues) =>
          val fields = rawRecordValues
            .groupBy(_.read[String]("field_name"))
            .map {
              case (fieldName, rawValues) =>
                (fieldName, rawValues.map(_.read[String]("value")).toArray.sorted)
            }
          RawRecord(id.toLong, fields + ("redcap_event_name" -> Array(eventName)))
      }
  }

  private def readJsonLists(ctx: ScioContext, inputPrefix: String): SCollection[Msg] = {
    StorageIO
      .readJsonLists(
        ctx,
        "Raw Records",
        s"${inputPrefix}/records/*.json"
      )
  }

}
