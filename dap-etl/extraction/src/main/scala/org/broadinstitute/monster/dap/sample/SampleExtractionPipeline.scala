package org.broadinstitute.monster.dap.sample

import org.broadinstitute.monster.common.{PipelineBuilder, ScioApp}
import org.broadinstitute.monster.dap.common._

import java.time.{OffsetDateTime, ZoneOffset}

// Ignore IntelliJ, this is used to make the implicit parser compile.
import Args._

object SampleExtractionPipeline extends ScioApp[Args] {

  // january 1, 2018 - we ignore any records before this by default (though there shouldn't be any)
  val SampleEpoch = OffsetDateTime.of(2018, 1, 1, 0, 0, 0, 0, ZoneOffset.ofHours(-5))

  val forms = List(
    "dna_kit_tracker",
    "cohort_enrollment",
    "recruitment_fields"
  )

  def extractionFiltersGenerator(args: Args): List[FilterDirective] = {
    val standardDirectives: List[FilterDirective] = List(
      // DAP Pack filters
      FilterDirective("st_dap_pack_count", FilterOps.>, "0"),
      FilterDirective("st_vip_or_staff", FilterOps.==, "0")
    )
    val dateFilters: List[FilterDirective] = {
      args.startTime
        .map(start =>
          List(
            FilterDirective("k1_rtn_tracking_date", FilterOps.>, RedCapClient.redcapFormatDate(start))
          )
        )
        .getOrElse(List()) ++
        args.endTime
          .map(end =>
            List(
              FilterDirective("k1_rtn_tracking_date", FilterOps.<, RedCapClient.redcapFormatDate(end))
            )
          )
          .getOrElse(List())
    }
    standardDirectives ++ dateFilters
  }

  val subdir = "sample";
  val arm = "baseline_arm_1"
  val fieldList = List("k1_tube_serial", "k1_rtn_tracking_date")

  def buildPipelineWithWrapper(wrapper: HttpWrapper): PipelineBuilder[Args] =
    new ExtractionPipelineBuilder(
      forms,
      extractionFiltersGenerator,
      (_, _) => List(arm),
      fieldList,
      subdir,
      100,
      RedCapClient.apply(_: List[String], wrapper)
    )

  override def pipelineBuilder: PipelineBuilder[Args] = buildPipelineWithWrapper(new OkWrapper())
}
