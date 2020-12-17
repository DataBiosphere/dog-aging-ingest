package org.broadinstitute.monster.dap

import com.spotify.scio.ScioContext
import com.spotify.scio.transforms.ScalaAsyncLookupDoFn
import org.apache.beam.sdk.coders.{KvCoder, StringUtf8Coder}
import org.apache.beam.sdk.transforms.{GroupIntoBatches, ParDo}
import org.apache.beam.sdk.values.KV
import org.broadinstitute.monster.common.{PipelineBuilder, StorageIO}
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
  * @param arm List of event arms to be pulled from RedCap (optional)
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
  arm: List[String],
  fieldList: List[String],
  subDir: String,
  idBatchSize: Int,
  getClient: List[String] => RedCapClient
) extends PipelineBuilder[Args]
    with Serializable {

  override def buildPipeline(ctx: ScioContext, args: Args): Unit = {
    import org.broadinstitute.monster.common.msg.MsgOps
    import ExtractionPipelineBuilder._

    val lookupFn =
      new ScalaAsyncLookupDoFn[RedcapRequest, Msg, RedCapClient](MaxConcurrentRequests) {
        override def newClient(): RedCapClient = getClient(arm)
        override def asyncLookup(
          client: RedCapClient,
          input: RedcapRequest
        ): Future[Msg] =
          client.get(args.apiToken, input)
      }

    // build a request to grab all study IDs for the data type we've specified in the given time windows
    val initRequest = GetRecords(
      fields = List("study_id"),
      start = args.startTime,
      end = args.endTime,
      filters = extractionFilters
    )
    val idsToExtract = ctx
    // massaging data to get back an SCollection[]
      .parallelize(Iterable(initRequest))
      .transform("Get study IDs") {
        // ParDo.of(lookupFn) runs the query to redcap via Beam and returns the results in batches,
        // this transform digs through each batch to yank out the values (the study IDs) and flattens
        // them into a single list
        _.applyKvTransform(ParDo.of(lookupFn)).flatMap { kv =>
          kv.getValue.fold(throw _, _.arr.map(_.read[String]("value")))
        }
      }

    // Group downloaded IDs into batches.
    // NOTE: This logic is replicated from the encode-ingest pipeline,
    // we should consider moving it to scio-utils.
    val batchedIds = idsToExtract
      // Pack up the IDs into dummy objects so beam knows how to handle them,
      // then encode them into UTF8 so they make consistent JSON.
      .map(KV.of("", _))
      .setCoder(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()))
      // Batch up the IDs
      .applyKvTransform(GroupIntoBatches.ofSize(idBatchSize.toLong))
      .map { ids =>
        // Construct a query to grab the full records for each batch of study IDs
        GetRecords(
          ids = ids.getValue.asScala.toList,
          forms = formsForExtraction,
          fields = fieldList
        )
      }

    // Flatten the batches of results into a single, massive list once they've come back
    val extractedRecords = batchedIds.transform("Get records") {
      _.applyKvTransform(ParDo.of(lookupFn)).flatMap(kv => kv.getValue.fold(throw _, _.arr))
    }

    if (args.pullDataDictionaries) {
      // Download the data dictionary for every form we requested.
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
