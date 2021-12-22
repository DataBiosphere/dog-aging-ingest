package org.broadinstitute.monster.dap.afus

import com.spotify.scio.ScioContext
import com.spotify.scio.values.SCollection
import org.broadinstitute.monster.common.{PipelineBuilder, StorageIO}
import org.broadinstitute.monster.dap.common._
import org.slf4j.{Logger, LoggerFactory}

object AfusTransformationPipelineBuilder extends PipelineBuilder[Args] {
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

    val owners =
      rawRecords.transform("Map AFUS Owners")(_.flatMap(AfusOwnerTransformations.mapAfusOwnerData))
    val dogs =
      rawRecords.transform("Map AFUS Dogs")(_.flatMap(AfusDogTransformations.mapAfusDog))

    StorageIO.writeJsonLists(owners, "AFUS Owners", s"${args.outputPrefix}/afus_owner")
    StorageIO.writeJsonLists(dogs, "AFUS Dogs", s"${args.outputPrefix}/afus_dog")
    ()
  }

  /** Read in records and group by study Id, with field name subgroups. */
  def readRecords(ctx: ScioContext, args: Args): SCollection[RawRecord] = {
    TransformationHelper.readRecordsGroupByStudyId(ctx, args.inputPrefix)
  }
}
