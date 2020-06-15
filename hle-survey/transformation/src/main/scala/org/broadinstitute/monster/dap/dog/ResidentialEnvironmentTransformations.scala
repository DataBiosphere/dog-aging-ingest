package org.broadinstitute.monster.dap.dog

import org.broadinstitute.monster.dap.RawRecord
import org.broadinstitute.monster.dogaging.jadeschema.fragment.HlesDogResidentialEnvironment

object ResidentialEnvironmentTransformations {

  /**
    * Parse all residential environment fields out of a raw RedCap record,
    * injecting them into a partially-modeled dog record.
    */
  def mapResidentialEnvironment(rawRecord: RawRecord): HlesDogResidentialEnvironment =
    HlesDogResidentialEnvironment.init()

}
