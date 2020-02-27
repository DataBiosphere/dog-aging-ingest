package org.broadinstitute.monster.dap

import org.broadinstitute.monster.common.{PipelineBuilder, ScioApp}

// Ignore IntelliJ, this is used to make the implicit parser compile.
import Args._

/** TODO */
object HLESurveyExtractionPipeline extends ScioApp[Args] {
  /** TODO */
  val BatchSize = 100

  /** TODO */
  val ExtractedForms = Set(
    "recruitment_fields",
    "owner_contact",
    "owner_demographics",
    "dog_demographics",
    "environment",
    "physical_activity",
    "behavior",
    "diet",
    "meds_and_preventives",
    "health_status"
  )

  override def pipelineBuilder: PipelineBuilder[Args] =
    new HLESurveyExtractionPipelineBuilder(BatchSize, ExtractedForms, RedCapClient.apply)
}
