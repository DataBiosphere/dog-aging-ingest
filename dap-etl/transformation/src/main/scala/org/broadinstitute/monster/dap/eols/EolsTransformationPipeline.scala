package org.broadinstitute.monster.dap.eols

import com.spotify.scio.ScioResult
import org.broadinstitute.monster.common.{PipelineBuilder, ScioApp}
import org.broadinstitute.monster.dap.common.{Args, PostProcess}

/** Entry-point for the end of life survey transformation pipeline. */
object EolsTransformationPipeline extends ScioApp[Args] {

  override def pipelineBuilder: PipelineBuilder[Args] = EolsTransformationPipelineBuilder
  override def postProcess: ScioResult => Unit = PostProcess.postProcess
}
