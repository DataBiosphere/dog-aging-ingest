package org.broadinstitute.monster.dap

import com.spotify.scio.ScioContext
import com.spotify.scio.coders.Coder
import com.spotify.scio.values.SCollection
import org.broadinstitute.monster.common.{PipelineBuilder, StorageIO}
import org.broadinstitute.monster.common.msg._
import org.broadinstitute.monster.dogaging.jadeschema.table.{HlesDog, HlesOwner}
import upack.Msg

object HLESurveyTransformationPipelineBuilder extends PipelineBuilder[Args] {

  implicit val msgCoder: Coder[Msg] = Coder.beam(new UpackMsgCoder)

  type RawRecord = Map[String, Array[String]]

  /**
    * Schedule all the steps for the Dog Aging transformation in the given pipeline context.
    *
    * Scheduled steps are launched against the context's runner when the `run()` method
    * is called on it.
    */
  override def buildPipeline(ctx: ScioContext, args: Args): Unit = {
    val rawRecords = readRecords(ctx, args)
    val dogs = rawRecords.transform("Map Dogs")(_.map(mapDog))
    val owners = rawRecords.transform("Map Owners")(_.map(mapOwner))

    StorageIO.writeJsonLists(dogs, "Dogs", s"${args.outputPrefix}/hles_dog")
    StorageIO.writeJsonLists(owners, "Owners", s"${args.outputPrefix}/hles_owner")
  }

  /**
    * Read in records and group by study Id, with field name subgroups.
    * Output the format: (studyId, Iterable[(fieldName, Iterable[value])])
    */
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
      .values
      .map { rawRecordValues =>
        rawRecordValues
          .groupBy(_.read[String]("field_name"))
          .map {
            case (fieldName, rawValues) =>
              (fieldName, rawValues.map(_.read[String]("value")).toArray)
          }
      }
  }

  def mapOwner(rawRecord: RawRecord): HlesOwner = ???

  def mapDog(rawRecord: RawRecord): HlesDog = ???
}
