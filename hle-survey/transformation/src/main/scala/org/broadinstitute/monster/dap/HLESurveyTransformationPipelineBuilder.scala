package org.broadinstitute.monster.dap.transformation

import com.spotify.scio.ScioContext
import org.broadinstitute.monster.common.{PipelineBuilder, StorageIO}
import org.broadinstitute.monster.common.msg._

object HLESurveyTransformationPipelineBuilder extends PipelineBuilder[Args] {
  override def buildPipeline(ctx: ScioContext, args: Args): Unit = {

    // Read records in
    val recordsDirectoryName = "records"
    val rawValueRecords = StorageIO
      .readJsonLists(ctx, recordsDirectoryName, s"${args.inputPrefix}/$recordsDirectoryName/*.json")

    // Group by study ID (record number)
    val valuesByStudyId = rawValueRecords
      .groupBy(_.read[String]("record"))

    // Group by field name
    // Transform the double-grouped collections of EAV records into upack Msgs with:
      // A key per field-name group, and
      // The raw value(s) of the field-name-key as the object value
  }
}
