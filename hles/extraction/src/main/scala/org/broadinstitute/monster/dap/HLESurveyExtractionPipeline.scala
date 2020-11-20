package org.broadinstitute.monster.dap

import org.broadinstitute.monster.common.{PipelineBuilder, ScioApp}

// Ignore IntelliJ, this is used to make the implicit parser compile.
import Args._

/** Entry-point for the HLE extraction pipeline. */
object HLESurveyExtractionPipeline extends ScioApp[Args] {

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

  val extractionFilters: Map[String, String] = forms
    .filterNot(_ == "study_status") // For some reason, study_status is never marked as completed.
    .map(form => s"${form}_complete" -> "2") // Magic marker for "completed".
    .toMap + ("co_consent" -> "1")

  val jobTag = "hles"

  override def pipelineBuilder: PipelineBuilder[Args] =
    // Use a batch size of 100 because it seems to work well enough.
    // We might need to revisit this as more dogs are consented.
    new ExtractionPipelineBuilder(forms, extractionFilters, jobTag, 100, RedCapClient.apply)
}
