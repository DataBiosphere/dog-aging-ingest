package org.broadinstitute.monster.dap.environment

import org.broadinstitute.monster.dap.common.RawRecord
import org.broadinstitute.monster.dogaging.jadeschema.fragment.EnvironmentGeocoding

object GeocodingTransformations {

  /**
    * Parse all geocoding_metadata variables out of a raw RedCap record,
    * injecting them into a partially-modeled environment record.
    */
  def mapGeocodingMetadata(rawRecord: RawRecord): EnvironmentGeocoding = {
    EnvironmentGeocoding(
      gmAddressType = rawRecord.getOptionalNumber("gm_addr_type"),
      gmMatchType = rawRecord.getOptionalNumber("gm_match_type"),
      gmStateFips = rawRecord.getOptional("gm_statefp"),
      gmGeocoder = rawRecord.getOptionalNumber("gm_geocoder"),
      gmEntryType = rawRecord.getOptionalNumber("gm_entry_type"),
      gmComplete = rawRecord.getOptionalNumber("geocoding_metadata_complete")
    )
  }
}
