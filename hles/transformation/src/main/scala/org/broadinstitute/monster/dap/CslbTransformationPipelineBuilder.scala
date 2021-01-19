package org.broadinstitute.monster.dap

import com.spotify.scio.ScioContext
import com.spotify.scio.values.SCollection
import org.broadinstitute.monster.common.{PipelineBuilder, StorageIO}
import org.broadinstitute.monster.common.msg._
import org.broadinstitute.monster.dogaging.jadeschema.table.Cslb
import org.slf4j.{Logger, LoggerFactory}

object CslbTransformationPipelineBuilder extends PipelineBuilder[Args] {
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
    implicit def lw: LabelledWrite[Cslb] = deriveLabelledWrite

    val cslbTransformations =
      rawRecords.transform("CSLB data")(_.flatMap(CslbTransformations.mapCslbData))


    // todo: primary key of entity:dog_id_id must be added (replacing the dog_id column) as the first column
    cslbTransformations
      .transform("CSLB to TSV reformatting")(_.map(CslbTransformations.mapToTsvFormat))
      .transform("CSLB TSV serialization")(record => {
        val result: CSV.Row = record.write
        result.print(Printer.tsv)
      })
      .saveAsTextFile(
        s"${args.outputPrefix}/cslb_tsv",
        numShards = 1,
        header = Some(lw.headers.print(Printer.tsv))
      )

    val caseClassHeaders = lw.headers.productElementNames.intoList
    val caseless = caseClassHeaders.remove("dog_id")

    StorageIO.writeJsonLists(
      cslbTransformations,
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
}
