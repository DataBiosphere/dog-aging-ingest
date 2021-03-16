package org.broadinstitute.monster.dap

import com.spotify.scio.ScioMetrics.counter
import org.broadinstitute.monster.dap.common.PostProcess.errCount
import org.slf4j.Logger
import ujson.Obj

// A trait to capture the generic logging mechanism we'll want with case classes for the different logging levels.
//   To note a skippable error you encountered, you should just call the `log` method of these classes.
//   They should only be thrown in cases where a missing field means the entire record must be thrown out, and you should
//     still call `log` in the block where you catch the error and skip the record.
abstract class HLESurveyTransformationLog extends Exception {
  val jsonMsg: Obj

  def log(implicit logger: Logger): Unit
}

class HLESurveyTransformationError(msg: String) extends HLESurveyTransformationLog {

  def log(implicit logger: Logger): Unit = {
    logger.error(jsonMsg.toString())
    counter("main", errCount).inc()
  }

  val jsonMsg: Obj = ujson
    .Obj(
      "errorType" -> ujson.Str(this.getClass.getSimpleName),
      "message" -> ujson.Str(msg)
    )
}

// case classes for all the different actual warnings and errors we want to raise during the workflow
case class MissingOwnerIdError(msg: String) extends HLESurveyTransformationError(msg)
case class TruncatedDecimalError(msg: String) extends HLESurveyTransformationError(msg)
case class MissingCalcFieldError(msg: String) extends HLESurveyTransformationError(msg)
case class InvalidArmMonthError(msg: String) extends HLESurveyTransformationError(msg)
