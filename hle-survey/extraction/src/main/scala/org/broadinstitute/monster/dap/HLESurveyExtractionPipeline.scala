package org.broadinstitute.monster.dap

import org.broadinstitute.monster.common.{PipelineBuilder, ScioApp}

// Ignore IntelliJ, this is used to make the implicit parser compile.
import Args._

object HLESurveyExtractionPipeline extends ScioApp[Args] {

  override def pipelineBuilder: PipelineBuilder[Args] =
    new HLESurveyExtractionPipelineBuilder(RedCapClient.apply)
}
