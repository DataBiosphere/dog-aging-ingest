package org.broadinstitute.monster.dap

import java.time.OffsetDateTime

import com.spotify.scio.ScioContext
import com.spotify.scio.coders.Coder
import com.spotify.scio.transforms.ScalaAsyncLookupDoFn
import org.apache.beam.sdk.transforms.ParDo
import org.broadinstitute.monster.common.msg.UpackMsgCoder
import org.broadinstitute.monster.common.{PipelineBuilder, StorageIO}
import upack.Msg

import scala.concurrent.Future

class HLESurveyExtractionPipelineBuilder(getClient: () => RedCapClient)
    extends PipelineBuilder[Args]
    with Serializable {
  implicit val msgCoder: Coder[Msg] = Coder.beam(new UpackMsgCoder)

  implicit val odtCoder: Coder[OffsetDateTime] = Coder.xmap(Coder.stringCoder)(
    OffsetDateTime.parse,
    _.toString
  )

  override def buildPipeline(ctx: ScioContext, args: Args): Unit = {
    import org.broadinstitute.monster.common.msg.MsgOps

    val idLookupFn: ScalaAsyncLookupDoFn[Unit, Msg, RedCapClient] =
      new ScalaAsyncLookupDoFn[Unit, Msg, RedCapClient]() {
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
      .transform("Get consented records in time range") {
        _.applyKvTransform(ParDo.of(idLookupFn)).flatMap { kv =>
          kv.getValue.fold(throw _, _.arr.map(_.read[String]("study_id")))
        }
      }

    StorageIO.writeJsonLists(idsToExtract, "Consented IDs", s"${args.outputPrefix}/ids")
    ()
  }
}
