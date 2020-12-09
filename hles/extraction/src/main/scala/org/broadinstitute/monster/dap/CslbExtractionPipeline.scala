package org.broadinstitute.monster.dap

import org.broadinstitute.monster.common.{PipelineBuilder, ScioApp}

// Ignore IntelliJ, this is used to make the implicit parser compile.
import Args._

object CslbExtractionPipeline extends ScioApp[Args] {

  val forms = List(
    "recruitment_fields",
    "canine_social_and_learned_behavior"
  )

  // Magic marker for "completed".
  // NB: We are purposefully excluding the recruitment_fields_complete -> 2
  // mapping, as that conflicts with the CSLB data
  val extractionFilters: List[FilterDirective] = List(
    FilterDirective("canine_social_and_learned_behavior_complete", FilterOps.==, "2")
  )

  val subdir = "cslb"
  val arm = List("annual_2020_arm_1")
  val fieldList = List("co_consent")

  override def pipelineBuilder: PipelineBuilder[Args] =
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
