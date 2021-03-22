package org.broadinstitute.monster.dap.hles

import io.circe.Printer
import com.spotify.scio.ScioContext
import com.spotify.scio.values.SCollection
import org.broadinstitute.monster.common.{PipelineBuilder, StorageIO}
import org.broadinstitute.monster.dap.common._
import org.broadinstitute.monster.dap.dog.DogTransformations
import org.slf4j.{Logger, LoggerFactory}
import org.broadinstitute.monster.common.msg._

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

  // we save space by nixing whitespace, but we include null values to ensure that
  // we don't accidentally drop fields, regardless of whether any of them have been
  // filled in in our input data
  val printer: Printer = Printer.noSpaces.copy(dropNullValues = false)

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

    StorageIO.writeJsonLists(dogs, "Dogs", s"${args.outputPrefix}/hles_dog", printer = printer)
    StorageIO.writeJsonLists(owners, "Owners", s"${args.outputPrefix}/hles_owner", printer = printer)
    StorageIO.writeJsonLists(
      cancerConditions,
      "Cancer conditions",
      s"${args.outputPrefix}/hles_cancer_condition",
      printer = printer
    )
    StorageIO.writeJsonLists(
      healthConditions,
      "Health conditions",
      s"${args.outputPrefix}/hles_health_condition",
      printer = printer
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

    // Group by study ID (record number) and field name
    // to get the format: (studyId, Iterable((fieldName, Iterable(value))))
    rawRecords
      .groupBy(_.read[String]("record"))
      .map {
        case (id, rawRecordValues) =>
          val fields = rawRecordValues
            .groupBy(_.read[String]("field_name"))
            .map {
              case (fieldName, rawValues) =>
                (fieldName, rawValues.map(_.read[String]("value")).toArray.sorted)
            }
          RawRecord(id.toLong, fields)
      }
  }

}
