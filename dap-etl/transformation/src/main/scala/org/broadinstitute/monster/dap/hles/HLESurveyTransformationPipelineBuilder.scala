package org.broadinstitute.monster.dap.hles

import com.spotify.scio.ScioContext
import com.spotify.scio.values.SCollection
import org.broadinstitute.monster.common.{PipelineBuilder, StorageIO}
import org.broadinstitute.monster.dap.common._
import org.broadinstitute.monster.dap.dog.DogTransformations
import org.slf4j.{Logger, LoggerFactory}

object HLESurveyTransformationPipelineBuilder extends PipelineBuilder[Args] {
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

    val dogs = rawRecords.transform("Map Dogs")(_.flatMap(DogTransformations.mapDog))
    val owners = rawRecords.transform("Map Owners")(_.flatMap(OwnerTransformations.mapOwner))
    val cancerConditions =
      rawRecords.transform("Map Cancer conditions")(
        _.flatMap(CancerTransformations.mapCancerConditions)
      )
    val healthConditions = rawRecords.transform("Map health conditions")(
      _.flatMap(HealthTransformations.mapHealthConditions)
    )

    StorageIO.writeJsonLists(dogs, "Dogs", s"${args.outputPrefix}/hles_dog")
    StorageIO.writeJsonLists(owners, "Owners", s"${args.outputPrefix}/hles_owner")
    StorageIO.writeJsonLists(
      cancerConditions,
      "Cancer conditions",
      s"${args.outputPrefix}/hles_cancer_condition"
    )
    StorageIO.writeJsonLists(
      healthConditions,
      "Health conditions",
      s"${args.outputPrefix}/hles_health_condition"
    )
    ()
  }

  /** Read in records and group by study Id, with field name subgroups. */
  def readRecords(ctx: ScioContext, args: Args): SCollection[RawRecord] = {
    TransformationHelper.readRecordsGroupByEventName(ctx, args.inputPrefix)
  }

}
