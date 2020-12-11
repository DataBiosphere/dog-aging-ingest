package org.broadinstitute.monster.dap

import com.spotify.scio.ScioContext
import com.spotify.scio.values.SCollection
import org.broadinstitute.monster.common.{PipelineBuilder, StorageIO}
import org.broadinstitute.monster.common.msg._
import org.slf4j.{Logger, LoggerFactory}
import upack.Msg

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
    val rawEnvRecords = readEnvRecords(ctx,args)

    val dogs = rawRecords.transform("Map Dogs")(_.map(DogTransformations.mapDog))
    val owners = rawRecords.transform("Map Owners")(_.map(OwnerTransformations.mapOwner))
    val cancer_conditions =
      rawRecords.transform("Map Cancer conditions")(
        _.flatMap(CancerTransformations.mapCancerConditions)
      )
    val health_conditions = rawRecords.transform("Map health conditions")(
      _.flatMap(HealthTransformations.mapHealthConditions)
    )
    val environment =
      rawEnvRecords.transform("Map Environment")(_.map(EnvironmentTransformations.mapEnvironment))
    val cslb_transformations =
      rawRecords.transform("CSLB data")(_.flatMap(CslbTransformations.mapCslbData))

    StorageIO.writeJsonLists(dogs, "Dogs", s"${args.outputPrefix}/hles_dog")
    StorageIO.writeJsonLists(owners, "Owners", s"${args.outputPrefix}/hles_owner")
    StorageIO.writeJsonLists(
      cancer_conditions,
      "Cancer conditions",
      s"${args.outputPrefix}/hles_cancer_condition"
    )
    StorageIO.writeJsonLists(
      health_conditions,
      "Health conditions",
      s"${args.outputPrefix}/hles_health_condition"
    )
    StorageIO.writeJsonLists(
      environment,
      "Environmental data",
      s"${args.outputPrefix}/environment"
    )
    StorageIO.writeJsonLists(
      cslb_transformations,
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

  /** Read in records and group by study Id, with field name subgroups. */
  def readEnvRecords(ctx: ScioContext, args: Args): SCollection[RawRecord] = {
    val rawRecords: SCollection[Msg] = StorageIO
      .readJsonLists(
        ctx,
        "Raw Records",
        s"${args.inputPrefix}/records/*.json"
      )

    // Group by study ID (record number) and arm (address_year_month)
    // to get the format: (studyId, arm_id, Iterable((fieldName, Iterable(value))))
    // (study_id, arm_id, iterable())
    rawRecords
      .groupBy(foo => {
        (foo.read[String]("record"), foo.read[String]("redcap_event_name"))
      })
      //.groupBy(read[String]("redcap_event_name"))
      .map {
        case ((id, eventName), rawRecordValues) =>
          val fields: Map[String, Array[String]] = rawRecordValues
            .groupBy(_.read[String]("field_name"))
            .map {
              case (fieldName, rawValues) =>
                (fieldName, rawValues.map(_.read[String]("value")).toArray.sorted)
            }
          val bar = fields + ("redcap_event_name" -> Array(eventName))
          RawRecord(id.toLong, bar)
      }
  }
}
