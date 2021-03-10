package org.broadinstitute.monster.dap.dog

import org.broadinstitute.monster.dap.RawRecord
import org.broadinstitute.monster.dap.environment.GeocodingTransformations
import org.scalatest.OptionValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class GeocodingTransformationsSpec extends AnyFlatSpec with Matchers with OptionValues {

  behavior of "GeocodingTransformations"

  it should "map geocoding metadata variables where complete" in {
    val geocodingMetadata = Map(
      "gm_addr_type" -> Array("2"),
      "gm_match_type" -> Array("1"),
      "gm_statefp" -> Array("53"),
      "gm_geocoder" -> Array("1"),
      "gm_entry_type" -> Array("1"),
      "geocoding_metadata_complete" -> Array("2")
    )

    val geocodingDataMapped = GeocodingTransformations.mapGeocodingMetadata(
      RawRecord(1, geocodingMetadata)
    )

    // output of the example record's geocoding transformations
    geocodingDataMapped.gmAddressType.value shouldBe 2L
    geocodingDataMapped.gmMatchType.value shouldBe 1L
    geocodingDataMapped.gmStateFips shouldBe Some("53")
    geocodingDataMapped.gmGeocoder.value shouldBe 1L
    geocodingDataMapped.gmEntryType.value shouldBe 1L
    geocodingDataMapped.gmComplete.value shouldBe 2L
  }
}
