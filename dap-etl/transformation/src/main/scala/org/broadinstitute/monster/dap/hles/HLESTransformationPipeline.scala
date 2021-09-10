package org.broadinstitute.monster.dap.hles

import com.spotify.scio.ScioResult
import org.broadinstitute.monster.common.{PipelineBuilder, ScioApp}
import org.broadinstitute.monster.dap.common.{Args, PostProcess}

/** Entry-point for the HLE transformation pipeline. */
object HLESTransformationPipeline extends ScioApp[Args] {

  override def pipelineBuilder: PipelineBuilder[Args] = HLESurveyTransformationPipelineBuilder
  override def postProcess: ScioResult => Unit = PostProcess.postProcess
}
