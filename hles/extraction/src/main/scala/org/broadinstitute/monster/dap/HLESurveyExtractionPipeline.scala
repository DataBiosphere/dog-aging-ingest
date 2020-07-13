package org.broadinstitute.monster.dap

import org.broadinstitute.monster.common.{PipelineBuilder, ScioApp}

// Ignore IntelliJ, this is used to make the implicit parser compile.
import Args._

/** Entry-point for the HLE extraction pipeline. */
object HLESurveyExtractionPipeline extends ScioApp[Args] {

  override def pipelineBuilder: PipelineBuilder[Args] =
    // Use a batch size of 100 because it seems to work well enough.
    // We might need to revisit this as more dogs are consented.
    new HLESurveyExtractionPipelineBuilder(100, RedCapClient.apply)
}
