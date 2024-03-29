package org.broadinstitute.monster.dap.common

import com.spotify.scio.ScioResult

object PostProcess {

  val errCount = "errorCount"

  def postProcess(result: ScioResult): Unit = {
    result.allCounters.foreach {
      case (name, count) =>
        if (name.getName == errCount)
          count.committed.fold(())(count =>
            if (count > 0) throw new HLESurveyTransformationFailException
          )
    }
  }
}

class HLESurveyTransformationFailException extends Exception
