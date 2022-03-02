package org.broadinstitute.monster.dap.eols

import org.broadinstitute.monster.common.{PipelineBuilder, ScioApp}
import org.broadinstitute.monster.dap.common.{
  Args,
  ExtractionPipelineBuilder,
  FilterDirective,
  FilterOps,
  HttpWrapper,
  OkWrapper,
  RedCapClient
}

// Ignore IntelliJ, this is used to make the implicit parser compile.
import Args._
import java.time.{OffsetDateTime, ZoneOffset}

object EolsExtractionPipeline extends ScioApp[Args] {
  // january 1, 2018 - we ignore any records before this by default (though there shouldn't be any)
  val EOLSEpoch = OffsetDateTime.of(2018, 1, 1, 0, 0, 0, 0, ZoneOffset.ofHours(-5))

  val forms = List(
    "end_of_life"
  )

  def extractionFiltersGenerator(args: Args): List[FilterDirective] = {
    // make sure we are only collecting completed survey forms
    val completionFilters: List[FilterDirective] = forms
      .map(form => FilterDirective(s"${form}_complete", FilterOps.==, "2"))
    // EOLS consent
    val standardDirectives: List[FilterDirective] = List(
      FilterDirective("eol_willing_to_complete", FilterOps.==, "1"),
      // DAP Pack filters
      FilterDirective("st_dap_pack_count", FilterOps.>, "0"),
      FilterDirective("st_vip_or_staff", FilterOps.==, "0")
    )
    val dateFilters: List[FilterDirective] =
      args.startTime
        .map(start =>
          List(
            FilterDirective("eol_date_dog_died", FilterOps.>, RedCapClient.redcapFormatDate(start))
          )
        )
        .getOrElse(List()) ++
        args.endTime
          .map(end =>
            List(
              FilterDirective("eol_date_dog_died", FilterOps.<, RedCapClient.redcapFormatDate(end))
            )
          )
          .getOrElse(List())

    completionFilters ++ standardDirectives ++ dateFilters
  }

  val subdir = "eols";
  val arm = "baseline_arm_1"
  val fieldList = List("eol_willing_to_complete", "eol_date_dog_died")

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
