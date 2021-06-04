package org.broadinstitute.monster.dap.sample

import com.spotify.scio.ScioResult
import org.broadinstitute.monster.common.{PipelineBuilder, ScioApp}
import org.broadinstitute.monster.dap.common.{Args, PostProcess}

object SampleTransformationPipeline extends ScioApp[Args]{
  override def pipelineBuilder: PipelineBuilder[Args] = CslbTransformationPipelineBuilder
  override def postProcess: ScioResult => Unit = PostProcess.postProcess
}
