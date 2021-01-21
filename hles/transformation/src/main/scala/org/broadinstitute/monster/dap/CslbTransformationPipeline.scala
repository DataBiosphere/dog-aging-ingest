package org.broadinstitute.monster.dap

import com.spotify.scio.ScioResult
import org.broadinstitute.monster.common.{PipelineBuilder, ScioApp}

/** Entry-point for the CSLB transformation pipeline. */
object CslbTransformationPipeline extends ScioApp[Args] {

  override def pipelineBuilder: PipelineBuilder[Args] = CslbTransformationPipelineBuilder
  override def postProcess: ScioResult => Unit = PostProcess.postProcess
}
