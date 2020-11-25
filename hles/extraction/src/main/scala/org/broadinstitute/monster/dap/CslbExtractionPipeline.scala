package org.broadinstitute.monster.dap

import org.broadinstitute.monster.common.{PipelineBuilder, ScioApp}
import Args._

object CslbExtractionPipeline extends ScioApp[Args] {

  val forms = List(
    "recruitment_fields",
    "canine_social_and_learned_behavior"
  )

  // Magic marker for "completed".
  // NB: We are purposefully excluding the recruitment_fields_complete -> 2
  // mapping, as that conflicts with the CSLB data
  val extractionFilters: Map[String, String] = Map(
    s"canine_social_and_learned_behavior_complete" -> "2"
  )

  val subdir = "cslb"
  val arm = "annual_2020_arm_1"

  override def pipelineBuilder: PipelineBuilder[Args] =
    new ExtractionPipelineBuilder(
      forms,
      extractionFilters,
      arm,
      subdir,
      100,
      RedCapClient.apply
    )
}
