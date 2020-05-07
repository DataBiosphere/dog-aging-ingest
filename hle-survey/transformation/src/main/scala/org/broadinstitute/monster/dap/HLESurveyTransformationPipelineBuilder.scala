package org.broadinstitute.monster.dap

import com.spotify.scio.ScioContext
import org.broadinstitute.monster.common.PipelineBuilder

object HLESurveyTransformationPipelineBuilder extends PipelineBuilder[Args] {
  override def buildPipeline(ctx: ScioContext, args: Args): Unit = {}
}
