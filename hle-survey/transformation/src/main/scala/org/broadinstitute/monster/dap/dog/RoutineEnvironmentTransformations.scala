package org.broadinstitute.monster.dap.dog

import org.broadinstitute.monster.dap.RawRecord
import org.broadinstitute.monster.dogaging.jadeschema.fragment.HlesDogRoutineEnvironment

object RoutineEnvironmentTransformations {

  /**
    * Parse all routine environment fields out of a raw RedCap record,
    * injecting them into a partially-modeled dog record.
    */
  def mapRoutineEnvironment(rawRecord: RawRecord): HlesDogRoutineEnvironment =
    HlesDogRoutineEnvironment.init()

}
