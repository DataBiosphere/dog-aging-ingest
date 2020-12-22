package org.broadinstitute.monster.dap

import com.spotify.scio.ScioContext
import com.spotify.scio.transforms.ScalaAsyncLookupDoFn
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.coders.{KvCoder, StringUtf8Coder}
import org.apache.beam.sdk.transforms.{GroupIntoBatches, ParDo}
import org.apache.beam.sdk.values.KV
import org.broadinstitute.monster.common.{PipelineBuilder, StorageIO}
import org.slf4j.LoggerFactory
import upack.Msg

import scala.concurrent.Future
import scala.collection.JavaConverters._

object ExtractionPipelineBuilder {

  val MaxConcurrentRequests = 8
}

/**
  * Builder for an extraction pipeline.
  *
  * Sets up the pipeline to:
  *   1. Query the study IDs of all dogs, possibly within a fixed
  *      time frame
  *   2. Group the returned IDs into batches
  *   3. Download the values of all HLE forms for each batch of IDs
  *   4. Write the downloaded forms to storage
  *
  * @param formsForExtraction List of forms to be pulled from RedCap
  * @param extractionFilters  Map of filters to be applied whenn pulling RedCap data
  * @param arms List of event arms to be pulled from RedCap (optional)
  * @param fieldList List of event arms to be pulled from RedCap (optional)
  * @param subDir             : Sub directory name where data from this pipeline should be
  *                           written
  * @param idBatchSize max number of IDs to include per batch when
  *                    downloading record data
  * @param getClient function that will produce a client which can
  *                  interact with a RedCap API
  */
class ExtractionPipelineBuilder(
  formsForExtraction: List[String],
  extractionFilters: List[FilterDirective],
  arms: List[String],
  fieldList: List[String],
  subDir: String,
  idBatchSize: Int,
  getClient: () => RedCapClient
) extends PipelineBuilder[Args]
    with Serializable {

  override def buildPipeline(ctx: ScioContext, args: Args): Unit = {
    import org.broadinstitute.monster.common.msg.MsgOps
    import ExtractionPipelineBuilder._
    val logger = LoggerFactory.getLogger("extraction_pipeline")

    val lookupFn =
      new ScalaAsyncLookupDoFn[RedcapRequest, Msg, RedCapClient](MaxConcurrentRequests) {
        override def newClient(): RedCapClient = getClient()
        override def asyncLookup(
          client: RedCapClient,
          input: RedcapRequest
        ): Future[Msg] =
          client.get(args.apiToken, input)
      }

    // Dispatch requests for the list of records in each provided arm
    val initRequests: Seq[GetRecords] = arms.map(arm =>
      GetRecords(
        fields = List("study_id"),
        start = args.startTime,
        end = args.endTime,
        filters = extractionFilters,
        arm = List(arm)
      )
    )
    val idsToExtract: SCollection[String] = ctx
      .parallelize(initRequests)
      .transform(
        "Get study IDs for arm"
      ) {
        _.applyKvTransform(ParDo.of(lookupFn)).flatMap { kv =>
          kv.getValue.fold(throw _, _.arr.map(_.read[String]("value")))
        }
      }

    idsToExtract.count.map(cnt => logger.info(s"Will pull ${cnt} records"))

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
          forms = formsForExtraction,
          // List of fields to pull out of the data for us to filter on
          fields = fieldList
        )
      }

    // Download the form data for each batch of records.
    val extractedRecords = batchedIds.transform("Get records") {
      _.applyKvTransform(ParDo.of(lookupFn)).flatMap(kv => kv.getValue.fold(throw _, _.arr))
    }

    if (args.pullDataDictionaries) {
      // Download the data dictionary for every form.
      val extractedDataDictionaries = ctx
        .parallelize(formsForExtraction)
        .map(instrument => GetDataDictionary(instrument))
        .transform("Get data dictionary") {
          _.applyKvTransform(ParDo.of(lookupFn)).flatMap(kv => kv.getValue.fold(throw _, _.arr))
        }
      StorageIO.writeJsonListsGeneric(
        extractedDataDictionaries,
        "Write data dictionaries",
        s"${args.outputPrefix}/${subDir}/data_dictionaries"
      )
    }

    StorageIO.writeJsonListsGeneric(
      extractedRecords,
      "Write records",
      s"${args.outputPrefix}/${subDir}/records"
    )

    ()
  }
}
