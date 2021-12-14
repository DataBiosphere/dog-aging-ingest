package org.broadinstitute.monster.dap.afus

import com.spotify.scio.ScioResult
import org.broadinstitute.monster.common.{PipelineBuilder, ScioApp}
import org.broadinstitute.monster.dap.common.{Args, PostProcess}

/** Entry-point for the CSLB transformation pipeline. */
object AfusTransformationPipeline extends ScioApp[Args] {

  override def pipelineBuilder: PipelineBuilder[Args] = AfusTransformationPipelineBuilder
  override def postProcess: ScioResult => Unit = PostProcess.postProcess
}
