package org.broadinstitute.monster.dap.dog

import org.broadinstitute.monster.dap.RawRecord
import org.broadinstitute.monster.dogaging.jadeschema.fragment.HlesDogPhysicalActivity

object PhysicalActivityTransformations {

  /**
    * Parse all physical-activity-related fields out of a raw RedCap record,
    * injecting them into a partially-modeled dog record.
    */
  def mapPhysicalActivity(rawRecord: RawRecord): HlesDogPhysicalActivity =
    HlesDogPhysicalActivity.init()

  // break up into: overview, surface, walk, swim, other
}
