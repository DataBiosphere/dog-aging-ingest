package org.broadinstitute.monster.dap.eols

import com.spotify.scio.ScioContext
import com.spotify.scio.values.SCollection
import org.broadinstitute.monster.common.{PipelineBuilder, StorageIO}
import org.broadinstitute.monster.dap.common.{Args, RawRecord, TransformationHelper}
import org.slf4j.{Logger, LoggerFactory}

object EolsTransformationPipelineBuilder extends PipelineBuilder[Args] {
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

    val eols = rawRecords.transform("EOLS data")(_.map(EolsTransformations.mapEols))

    StorageIO.writeJsonLists(
      eols,
      "End of Life Survey data",
      s"${args.outputPrefix}/eols"
    )
    ()
  }

  /** Read in records and group by study Id, with field name subgroups. */
  def readRecords(ctx: ScioContext, args: Args): SCollection[RawRecord] = {
    TransformationHelper.readRecordsGroupByEventName(ctx, args.inputPrefix)
  }
}
