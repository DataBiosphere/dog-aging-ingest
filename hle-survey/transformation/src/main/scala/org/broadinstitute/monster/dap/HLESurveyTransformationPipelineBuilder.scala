package org.broadinstitute.monster.dap

import com.spotify.scio.ScioContext
import com.spotify.scio.values.SCollection
import org.broadinstitute.monster.common.{PipelineBuilder, StorageIO}
import org.broadinstitute.monster.common.msg._

object HLESurveyTransformationPipelineBuilder extends PipelineBuilder[Args] {

  /**
    * Schedule all the steps for the Dog Aging transformation in the given pipeline context.
    *
    * Scheduled steps are launched against the context's runner when the `run()` method
    * is called on it.
    */
  override def buildPipeline(ctx: ScioContext, args: Args): Unit = {

    /**
      * Read in records and group by study Id, with field name subgroups.
      * Output the format: (studyId, Iterable[(fieldName, Iterable[value])])
      */
    def readRecords(): SCollection[(String, Iterable[(String, Iterable[String])])] = {
      // Read records in
      val recordsDirectoryName = "records"
      val rawRecords = StorageIO
        .readJsonLists(
          ctx,
          recordsDirectoryName,
          s"${args.inputPrefix}/$recordsDirectoryName/*.json"
        )

      // Group by study ID (record number) and field name
      // to get the format: (studyId, Iterable((fieldName, Iterable(value))))
      rawRecords
        .groupBy(_.read[String]("record"))
        .map {
          case (studyId, rawRecordValues) =>
            val rawRecordsByFieldName =
              rawRecordValues.groupBy(_.read[String]("field_name")).map {
                case (fieldName, rawValues) =>
                  (fieldName, rawValues.map(_.read[String]("value")))
              }
            (studyId, rawRecordsByFieldName)
        }
    }
  }
}
