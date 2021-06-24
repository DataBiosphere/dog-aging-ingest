package org.broadinstitute.monster.dap.sample

import com.spotify.scio.ScioContext
import com.spotify.scio.values.SCollection
import org.broadinstitute.monster.common.{PipelineBuilder, StorageIO}
import org.broadinstitute.monster.dap.common.{Args, RawRecord, TransformationHelper}
import org.slf4j.{Logger, LoggerFactory}

object SampleTransformationPipelineBuilder extends PipelineBuilder[Args] {
  implicit val logger: Logger = LoggerFactory.getLogger(getClass)

  override def buildPipeline(ctx: ScioContext, args: Args): Unit = {
    val rawRecords = readRecords(ctx, args)

    val sampleTransformations =
      rawRecords.transform("Sample data")(_.flatMap(SampleTransformations.mapSampleData))

    StorageIO.writeJsonLists(
      sampleTransformations,
      "Sample data",
      s"${args.outputPrefix}/sample"
    )
    ()
  }

  /** Read in records and group by study Id, with field name subgroups. */
  def readRecords(ctx: ScioContext, args: Args): SCollection[RawRecord] = {
    TransformationHelper.readRecordsGroupByEventName(ctx, args.inputPrefix)
  }

}
