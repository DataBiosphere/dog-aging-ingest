package org.broadinstitute.monster.dap

import org.broadinstitute.monster.common.{PipelineBuilder, ScioApp}
import Args._

object CslbExtractionPipeline extends ScioApp[Args] {

  val forms = List(
    "canine_social_and_learned_behavior"
  )

  val extractionFilters: Map[String, String] = forms
    .map(form => s"${form}_complete" -> "2") // Magic marker for "completed".
    .toMap

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
