package org.broadinstitute.monster.dap

import org.broadinstitute.monster.common.{PipelineBuilder, ScioApp}

// Ignore IntelliJ, this is used to make the implicit parser compile.
import Args._

/** Entry-point for the HLE extraction pipeline. */
object HLESurveyExtractionPipeline extends ScioApp[Args] {

  /** Names of all forms we want to extract as part of HLE ingest. */
  val HLESEpoch = "2018-01-01"

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

  val extractionFilters: List[FilterDirective] = forms
    .filterNot(_ == "study_status") // For some reason, study_status is never marked as completed.
    .map(form => FilterDirective(s"${form}_complete", FilterOps.==, "2")) ++ List(
    FilterDirective("co_consent", FilterOps.==, "1"),
    FilterDirective("st_dap_pack_count", FilterOps.>, "0"),
    FilterDirective("st_dap_pack_date", FilterOps.>, HLESEpoch)
  ) // Magic marker for "completed".

  val subdir = "hles"
  // Limit to the initial HLE event.
  val arm = List("baseline_arm_1")
  val fieldList = List("co_consent")

  override def pipelineBuilder: PipelineBuilder[Args] =
    // Use a batch size of 100 because it seems to work well enough.
    // We might need to revisit this as more dogs are consented.
    new ExtractionPipelineBuilder(
      forms,
      extractionFilters,
      arm,
      fieldList,
      subdir,
      100,
      RedCapClient.apply
    )
}
