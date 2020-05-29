package org.broadinstitute.monster.dap

import java.time.OffsetDateTime

import com.spotify.scio.ScioContext
import com.spotify.scio.coders.Coder
import com.spotify.scio.transforms.ScalaAsyncLookupDoFn
import org.apache.beam.sdk.coders.{KvCoder, StringUtf8Coder}
import org.apache.beam.sdk.transforms.{GroupIntoBatches, ParDo}
import org.apache.beam.sdk.values.KV
import org.broadinstitute.monster.common.msg.UpackMsgCoder
import org.broadinstitute.monster.common.{PipelineBuilder, StorageIO}
import upack.Msg

import scala.concurrent.Future
import scala.collection.JavaConverters._

object HLESurveyExtractionPipelineBuilder {

  /** Names of all forms we want to extract as part of HLE ingest. */
  val ExtractedForms = List(
    "recruitment_fields",
    "owner_contact",
    "owner_demographics",
    "dog_demographics",
    "environment",
    "physical_activity",
    "behavior",
    "diet",
    "meds_and_preventives",
    "health_status",
    "additional_studies",
    "study_status"
  )

  val ExtractionFilters: Map[String, String] = ExtractedForms
    .filterNot(_ == "study_status") // For some reason, study_status is never marked as completed.
    .map(form => s"${form}_complete" -> "2") // Magic marker for "completed".
    .toMap + ("co_consent" -> "1")

  val MaxConcurrentRequests = 8
}

/**
  * Builder for the HLE extraction pipeline.
  *
  * Sets up the pipeline to:
  *   1. Query the study IDs of all dogs, possibly within a fixed
  *      time frame
  *   2. Group the returned IDs into batches
  *   3. Download the values of all HLE forms for each batch of IDs
  *   4. Write the downloaded forms to storage
  *
  * @param idBatchSize max number of IDs to include per batch when
  *                    downloading record data
  * @param getClient function that will produce a client which can
  *                  interact with a RedCap API
  */
class HLESurveyExtractionPipelineBuilder(
  idBatchSize: Int,
  getClient: () => RedCapClient
) extends PipelineBuilder[Args]
    with Serializable {
  implicit val msgCoder: Coder[Msg] = Coder.beam(new UpackMsgCoder)

  implicit val odtCoder: Coder[OffsetDateTime] = Coder.xmap(Coder.stringCoder)(
    OffsetDateTime.parse,
    _.toString
  )

  override def buildPipeline(ctx: ScioContext, args: Args): Unit = {
    import org.broadinstitute.monster.common.msg.MsgOps
    import HLESurveyExtractionPipelineBuilder._

    val lookupFn =
      new ScalaAsyncLookupDoFn[RedcapRequest, Msg, RedCapClient](MaxConcurrentRequests) {
        override def newClient(): RedCapClient = getClient()
        override def asyncLookup(
          client: RedCapClient,
          input: RedcapRequest
        ): Future[Msg] =
          client.get(args.apiToken, input)
      }

    // Start by pulling the IDs of all records that:
    //  1. Have consented to participate in HLES
    //  2. Have completed all the HLES forms we care about
    val initRequest = GetRecords(
      fields = List("study_id"),
      start = args.startTime,
      end = args.endTime,
      filters = ExtractionFilters
    )
    val idsToExtract = ctx
      .parallelize(Iterable(initRequest))
      .transform("Get study IDs") {
        _.applyKvTransform(ParDo.of(lookupFn)).flatMap { kv =>
          kv.getValue.fold(throw _, _.arr.map(_.read[String]("value")))
        }
      }

    // Group downloaded IDs into batches.
    // NOTE: This logic is replicated from the encode-ingest pipeline,
    // we should consider moving it to scio-utils.
    val batchedIds = idsToExtract
      .map(KV.of("", _))
      .setCoder(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()))
      .applyKvTransform(GroupIntoBatches.ofSize(idBatchSize.toLong))
      .map { ids =>
        GetRecords(
          ids = ids.getValue.asScala.toList,
          forms = ExtractedForms,
          // Pull the consent field so we can QC that the filter is working properly.
          fields = List("co_consent")
        )
      }

    // Download the form data for each batch of records.
    val extractedRecords = batchedIds.transform("Get HLE records") {
      _.applyKvTransform(ParDo.of(lookupFn)).flatMap(kv => kv.getValue.fold(throw _, _.arr))
    }

    // Download the data dictionary for every form.
    val extractedDataDictionaries = ctx
      .parallelize(ExtractedForms)
      .map(instrument => GetDataDictionary(instrument))
      .transform("Get data dictionary") {
        _.applyKvTransform(ParDo.of(lookupFn)).flatMap(kv => kv.getValue.fold(throw _, _.arr))
      }

    StorageIO.writeJsonLists(
      extractedRecords,
      "HLE Records",
      s"${args.outputPrefix}/records"
    )
    StorageIO.writeJsonLists(
      extractedDataDictionaries,
      "HLE Data Dictionaries",
      s"${args.outputPrefix}/data_dictionaries"
    )
    ()
  }
}
