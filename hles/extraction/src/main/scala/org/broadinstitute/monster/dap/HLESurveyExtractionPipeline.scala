package org.broadinstitute.monster.dap

import java.time.{OffsetDateTime, ZoneOffset}

import org.broadinstitute.monster.common.{PipelineBuilder, ScioApp}

// Ignore IntelliJ, this is used to make the implicit parser compile.
import Args._

/** Entry-point for the HLE extraction pipeline. */
object HLESurveyExtractionPipeline extends ScioApp[Args] {

  // january 1, 2018 - we ignore any records before this by default (though there shouldn't be any)
  val HLESEpoch = OffsetDateTime.of(2018, 1, 1, 0, 0, 0, 0, ZoneOffset.ofHours(-5))

  /** Names of all forms we want to extract as part of HLE ingest. */
  val forms = List(
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

  def extractionFiltersGenerator(args: Args): List[FilterDirective] = {
    val completionFilters: List[FilterDirective] = forms
      .filterNot(_ == "study_status") // For some reason, study_status is never marked as completed.
      // Magic marker for "completed".
      .map(form => FilterDirective(s"${form}_complete", FilterOps.==, "2"))
    val standardDirectives: List[FilterDirective] = List(
      FilterDirective("co_consent", FilterOps.==, "1"),
      FilterDirective("st_dap_pack_count", FilterOps.>, "0"),
      FilterDirective(
        "st_dap_pack_date",
        FilterOps.>,
        RedCapClient.redcapFormatDate(args.startTime.getOrElse(HLESEpoch))
      )
    )
    val endFilter: List[FilterDirective] =
      args.endTime
        .map(end =>
          List(FilterDirective("st_dap_pack_date", FilterOps.<, RedCapClient.redcapFormatDate(end)))
        )
        .getOrElse(List())

    completionFilters ++ standardDirectives ++ endFilter
  }

  val subdir = "hles"
  // Limit to the initial HLE event.
  val arm = List("baseline_arm_1")
  val fieldList = List("co_consent")

  def buildPipelineWithWrapper(wrapper: HttpWrapper): PipelineBuilder[Args] =
    // Use a batch size of 100 because it seems to work well enough.
    // We might need to revisit this as more dogs are consented.
    new ExtractionPipelineBuilder(
      forms,
      extractionFiltersGenerator,
      arm,
      fieldList,
      subdir,
      100,
      RedCapClient.apply(_: List[String], wrapper)
    )

  override def pipelineBuilder: PipelineBuilder[Args] = buildPipelineWithWrapper(new OkWrapper())
}
