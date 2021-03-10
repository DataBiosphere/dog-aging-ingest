package org.broadinstitute.monster.dap

import com.spotify.scio.ScioResult
import org.broadinstitute.monster.common.{PipelineBuilder, ScioApp}

/** Entry-point for the HLE transformation pipeline. */
object HLESurveyTransformationPipeline extends ScioApp[Args] {

  override def pipelineBuilder: PipelineBuilder[Args] = HLESurveyTransformationPipelineBuilder
  override def postProcess: ScioResult => Unit = PostProcess.postProcess
}
