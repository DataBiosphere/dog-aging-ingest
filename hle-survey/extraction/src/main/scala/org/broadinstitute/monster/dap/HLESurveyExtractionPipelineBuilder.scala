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

/** TODO */
class HLESurveyExtractionPipelineBuilder(
  idBatchSize: Int,
  formNames: Set[String],
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

    val idLookupFn = new ScalaAsyncLookupDoFn[Unit, Msg, RedCapClient]() {
      override def newClient(): RedCapClient = getClient()
      override def asyncLookup(
        client: RedCapClient,
        input: Unit
      ): Future[Msg] =
        client.getRecords(
          args.apiToken,
          fields = Set("study_id"),
          start = args.startTime,
          end = args.endTime,
          valueFilters = Map("co_consent" -> "1")
        )
    }

    val idsToExtract = ctx
      .parallelize(Iterable(()))
      .transform("Get study IDs") {
        _.applyKvTransform(ParDo.of(idLookupFn)).flatMap { kv =>
          kv.getValue.fold(throw _, _.arr.map(_.read[String]("study_id")))
        }
      }

    val batchedIds = idsToExtract
      .map(KV.of("", _))
      .setCoder(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()))
      .applyKvTransform(GroupIntoBatches.ofSize(idBatchSize.toLong))
      .map(_.getValue.asScala)

    val formLookupFn = new ScalaAsyncLookupDoFn[Iterable[String], Msg, RedCapClient]() {
      override def newClient(): RedCapClient = getClient()
      override def asyncLookup(
        client: RedCapClient,
        input: Iterable[String]
      ): Future[Msg] =
        client.getRecords(args.apiToken, ids = input.toSet, forms = formNames)
    }

    val extractedRecords = batchedIds.transform("Get HLE records") {
      _.applyKvTransform(ParDo.of(formLookupFn)).flatMap { kv =>
        kv.getValue.fold(throw _, _.arr)
      }
    }

    StorageIO.writeJsonLists(
      extractedRecords,
      "HLE Records",
      args.outputPrefix
    )
    ()
  }
}
