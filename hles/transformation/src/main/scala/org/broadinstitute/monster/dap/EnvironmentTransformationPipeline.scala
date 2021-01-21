package org.broadinstitute.monster.dap

import com.spotify.scio.ScioResult
import org.broadinstitute.monster.common.{PipelineBuilder, ScioApp}

/** Entry-point for the environment transformation pipeline. */
object EnvironmentTransformationPipeline extends ScioApp[Args] {

  override def pipelineBuilder: PipelineBuilder[Args] = EnvironmentTransformationPipelineBuilder
  override def postProcess: ScioResult => Unit = PostProcess.postProcess
}
