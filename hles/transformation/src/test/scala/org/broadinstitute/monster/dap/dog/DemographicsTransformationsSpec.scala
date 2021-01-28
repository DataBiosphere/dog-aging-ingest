package org.broadinstitute.monster.dap.dog

import org.broadinstitute.monster.dap.{MissingCalcFieldError, RawRecord}
import org.broadinstitute.monster.dogaging.jadeschema.fragment.HlesDogDemographics
import org.scalatest.OptionValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DemographicsTransformationsSpec extends AnyFlatSpec with Matchers with OptionValues {
  behavior of "DemographicsTransformations"

  it should "map pure-breed demographics fields" in {
    val akcExample = Map[String, Array[String]](
      "dd_dog_pure_or_mixed" -> Array("1"),
      "dd_dog_breed" -> Array("10"),
      "dd_dog_breed_non_akc" -> Array("This should not pass through"),
      "dd_dog_breed_mix_1" -> Array("Uh oh, this shouldn't be here!")
    )
    val nonAkcExample = Map[String, Array[String]](
      "dd_dog_pure_or_mixed" -> Array("1"),
      "dd_dog_breed" -> Array("277"),
      "dd_dog_breed_non_akc" -> Array("This should pass through")
    )

    val akcOut = DemographicsTransformations.mapBreed(
      RawRecord(id = 1, akcExample),
      HlesDogDemographics.init()
    )
    val nonAkcOut = DemographicsTransformations.mapBreed(
      RawRecord(id = 1, nonAkcExample),
      HlesDogDemographics.init()
    )

    akcOut.ddBreedPureOrMixed.value shouldBe 1L
    akcOut.ddBreedPure.value shouldBe 10L
    akcOut.ddBreedPureNonAkc shouldBe None
    akcOut.ddBreedMixedPrimary shouldBe None

    nonAkcOut.ddBreedPureOrMixed.value shouldBe 1L
    nonAkcOut.ddBreedPure.value shouldBe 277L
    nonAkcOut.ddBreedPureNonAkc.value shouldBe "This should pass through"
  }

  it should "map mixed-breed demographics fields" in {
    val example = Map[String, Array[String]](
      "dd_dog_pure_or_mixed" -> Array("2"),
      "dd_dog_breed" -> Array("wot, shouldn't be here"),
      "dd_dog_breed_mix_1" -> Array("11"),
      "dd_dog_breed_mix_2" -> Array("3")
    )
    val out = DemographicsTransformations.mapBreed(
      RawRecord(id = 1, example),
      HlesDogDemographics.init()
    )

    out.ddBreedPureOrMixed.value shouldBe 2L
    out.ddBreedPure shouldBe None
    out.ddBreedMixedPrimary.value shouldBe 11L
    out.ddBreedMixedSecondary.value shouldBe 3L
  }

  it should "map age-related demographics fields when birth year and month are known" in {
    val example = Map[String, Array[String]](
      "dd_dog_birth_year_certain" -> Array("1"),
      "dd_dog_current_year_calc" -> Array("2020"),
      "dd_dog_birth_year" -> Array("2010"),
      "dd_dog_current_month_calc" -> Array("5"),
      "dd_dog_birth_month_yn" -> Array("1"),
      "dd_dog_birth_month" -> Array("4"),
      "dd_dog_age_certain_why" -> Array("2", "4", "98"),
      "dd_dog_age_certain_other" -> Array("It is known")
    )
    val out = DemographicsTransformations.mapAge(
      RawRecord(id = 1, example),
      HlesDogDemographics.init()
    )

    // Make sure we're within a half-month of accuracy.
    out.ddAgeYears.value shouldBe (10d + 1 / 12d) +- 1 / 24d
    out.ddAgeBasis.value shouldBe 1L
    out.ddAgeExactSourceAcquiredAsPuppy.value shouldBe false
    out.ddAgeExactSourceRegistrationInformation.value shouldBe true
    out.ddAgeExactSourceDeterminedByRescueOrg.value shouldBe false
    out.ddAgeExactSourceDeterminedByVeterinarian.value shouldBe true
    out.ddAgeExactSourceFromLitterOwnerBred.value shouldBe false
    out.ddAgeExactSourceOther.value shouldBe true
    out.ddAgeExactSourceOtherDescription.value shouldBe "It is known"
    out.ddBirthMonthKnown.value shouldBe true
  }

  it should "map age-related demographics fields when birth year is known" in {
    val example = Map[String, Array[String]](
      "dd_dog_birth_year_certain" -> Array("1"),
      "dd_dog_current_year_calc" -> Array("2020"),
      "dd_dog_birth_year" -> Array("2010"),
      "dd_dog_current_month_calc" -> Array("5"),
      "dd_dog_birth_month_yn" -> Array("2"),
      "dd_dog_age_certain_why" -> Array("1", "3", "5")
    )
    val out = DemographicsTransformations.mapAge(
      RawRecord(id = 1, example),
      HlesDogDemographics.init()
    )

    // Make sure we're within a half-year of accuracy.
    out.ddAgeYears.value shouldBe 10d +- 1 / 2d
    out.ddAgeBasis.value shouldBe 2L
    out.ddAgeExactSourceAcquiredAsPuppy.value shouldBe true
    out.ddAgeExactSourceRegistrationInformation.value shouldBe false
    out.ddAgeExactSourceDeterminedByRescueOrg.value shouldBe true
    out.ddAgeExactSourceDeterminedByVeterinarian.value shouldBe false
    out.ddAgeExactSourceFromLitterOwnerBred.value shouldBe true
    out.ddAgeExactSourceOther.value shouldBe false
    out.ddBirthMonthKnown.value shouldBe false
  }

  it should "raise an exception when calculated current year is not supplied" in {
    val example = Map[String, Array[String]](
      "dd_dog_birth_year_certain" -> Array("1"),
      "dd_dog_birth_year" -> Array("2010"),
      "dd_dog_current_month_calc" -> Array("5"),
      "dd_dog_birth_month_yn" -> Array("2"),
      "dd_dog_age_certain_why" -> Array("1", "3", "5")
    )

    assertThrows[MissingCalcFieldError] {
      DemographicsTransformations.mapAge(RawRecord(id = 1, example), HlesDogDemographics.init())
    }
  }

  it should "raise an exception when calculated current month is not supplied" in {
    val example = Map[String, Array[String]](
      "dd_dog_birth_year_certain" -> Array("1"),
      "dd_dog_current_year_calc" -> Array("2020"),
      "dd_dog_birth_year" -> Array("2010"),
      "dd_dog_birth_month_yn" -> Array("2"),
      "dd_dog_age_certain_why" -> Array("1", "3", "5")
    )

    assertThrows[MissingCalcFieldError] {
      DemographicsTransformations.mapAge(RawRecord(id = 1, example), HlesDogDemographics.init())
    }
  }

  it should "map age-related demographics fields when age is estimated by owner" in {
    val example = Map[String, Array[String]](
      "dd_dog_birth_year_certain" -> Array("2"),
      "dd_dog_age" -> Array("15"),
      "dd_dog_age_estimate_why" -> Array("1", "2", "4", "98"),
      "dd_dog_age_estimate_other" -> Array("The dog told me")
    )
    val out = DemographicsTransformations.mapAge(
      RawRecord(id = 1, example),
      HlesDogDemographics.init()
    )

    out.ddAgeYears.value shouldBe 15d
    out.ddAgeBasis.value shouldBe 3L
    out.ddAgeEstimateSourceToldByPreviousOwner.value shouldBe true
    // Not a typo: The "2" above is apparently deprecated(?) and
    // doesn't map to anything.
    out.ddAgeEstimateSourceDeterminedByRescueOrg.value shouldBe false
    out.ddAgeEstimateSourceDeterminedByVeterinarian.value shouldBe true
    out.ddAgeEstimateSourceOther.value shouldBe true
    out.ddAgeEstimateSourceOtherDescription.value shouldBe "The dog told me"
  }

  it should "map sex-related demographics fields for neutered male dogs" in {
    val noLitters = Map[String, Array[String]](
      "dd_dog_sex" -> Array("1"),
      "dd_dog_spay_neuter" -> Array("1"),
      "dd_spay_or_neuter_age" -> Array("2"),
      "dd_ms_sired_yn" -> Array("2")
    )
    val litters = Map[String, Array[String]](
      "dd_dog_sex" -> Array("1"),
      "dd_dog_spay_neuter" -> Array("1"),
      "dd_spay_or_neuter_age" -> Array("2"),
      "dd_ms_sired_yn" -> Array("1"),
      "dd_mns_nbr_litters_2" -> Array("3")
    )
    val unknown = Map[String, Array[String]](
      "dd_dog_sex" -> Array("1"),
      "dd_dog_spay_neuter" -> Array("1"),
      "dd_spay_or_neuter_age" -> Array("2"),
      "dd_ms_sired_yn" -> Array("99")
    )

    val noLittersOut = DemographicsTransformations.mapSexSpayNeuter(
      RawRecord(1, noLitters),
      HlesDogDemographics.init()
    )
    val littersOut = DemographicsTransformations.mapSexSpayNeuter(
      RawRecord(1, litters),
      HlesDogDemographics.init()
    )
    val unknownOut = DemographicsTransformations.mapSexSpayNeuter(
      RawRecord(1, unknown),
      HlesDogDemographics.init()
    )

    noLittersOut.ddSex.value shouldBe 1L
    noLittersOut.ddSpayedOrNeutered.value shouldBe true
    noLittersOut.ddSpayOrNeuterAge.value shouldBe 2L
    noLittersOut.ddHasSiredLitters.value shouldBe 2L
    noLittersOut.ddLitterCount shouldBe None

    littersOut.ddSex.value shouldBe 1L
    littersOut.ddSpayedOrNeutered.value shouldBe true
    littersOut.ddSpayOrNeuterAge.value shouldBe 2L
    littersOut.ddHasSiredLitters.value shouldBe 1L
    littersOut.ddLitterCount.value shouldBe 3L

    unknownOut.ddSex.value shouldBe 1L
    unknownOut.ddSpayedOrNeutered.value shouldBe true
    unknownOut.ddSpayOrNeuterAge.value shouldBe 2L
    unknownOut.ddHasSiredLitters.value shouldBe 99L
    unknownOut.ddLitterCount shouldBe None
  }

  it should "map sex-related demographics fields for un-neutered male dogs" in {
    val noLitters = Map[String, Array[String]](
      "dd_dog_sex" -> Array("1"),
      "dd_dog_spay_neuter" -> Array("2"),
      "dd_mns_sired_yn" -> Array("2")
    )
    val litters = Map[String, Array[String]](
      "dd_dog_sex" -> Array("1"),
      "dd_dog_spay_neuter" -> Array("2"),
      "dd_mns_sired_yn" -> Array("1"),
      "dd_mns_nbr_litters" -> Array("3")
    )
    val unknown = Map[String, Array[String]](
      "dd_dog_sex" -> Array("1"),
      "dd_dog_spay_neuter" -> Array("2"),
      "dd_spay_or_neuter_age" -> Array("2"),
      "dd_mns_sired_yn" -> Array("99")
    )

    val noLittersOut = DemographicsTransformations.mapSexSpayNeuter(
      RawRecord(1, noLitters),
      HlesDogDemographics.init()
    )
    val littersOut = DemographicsTransformations.mapSexSpayNeuter(
      RawRecord(1, litters),
      HlesDogDemographics.init()
    )
    val unknownOut = DemographicsTransformations.mapSexSpayNeuter(
      RawRecord(1, unknown),
      HlesDogDemographics.init()
    )

    noLittersOut.ddSex.value shouldBe 1L
    noLittersOut.ddSpayedOrNeutered.value shouldBe false
    noLittersOut.ddHasSiredLitters.value shouldBe 2L
    noLittersOut.ddLitterCount shouldBe None

    littersOut.ddSex.value shouldBe 1L
    littersOut.ddSpayedOrNeutered.value shouldBe false
    littersOut.ddHasSiredLitters.value shouldBe 1L
    littersOut.ddLitterCount.value shouldBe 3L

    unknownOut.ddSex.value shouldBe 1L
    unknownOut.ddSpayedOrNeutered.value shouldBe false
    unknownOut.ddHasSiredLitters.value shouldBe 99L
    unknownOut.ddLitterCount shouldBe None
  }

  it should "map sex-related demographics fields for spayed female dogs" in {
    val noLitters = Map[String, Array[String]](
      "dd_dog_sex" -> Array("2"),
      "dd_dog_spay_neuter" -> Array("1"),
      "dd_spay_or_neuter_age" -> Array("2"),
      "dd_fs_spay_method" -> Array("3"),
      "dd_fs_pregnant" -> Array("2"),
      "dd_fs_heat_yn" -> Array("99")
    )
    val litters = Map[String, Array[String]](
      "dd_dog_sex" -> Array("2"),
      "dd_dog_spay_neuter" -> Array("1"),
      "dd_spay_or_neuter_age" -> Array("2"),
      "dd_fs_spay_method" -> Array("2"),
      "dd_fs_pregnant" -> Array("1"),
      "dd_fs_nbr_litters" -> Array("3"),
      "dd_fs_heat_yn" -> Array("2")
    )
    val unknown = Map[String, Array[String]](
      "dd_dog_sex" -> Array("2"),
      "dd_dog_spay_neuter" -> Array("1"),
      "dd_spay_or_neuter_age" -> Array("2"),
      "dd_fs_spay_method" -> Array("1"),
      "dd_fs_pregnant" -> Array("99"),
      "dd_fs_heat_yn" -> Array("1"),
      "dd_fs_nbr_cycles" -> Array("1")
    )

    val noLittersOut = DemographicsTransformations.mapSexSpayNeuter(
      RawRecord(1, noLitters),
      HlesDogDemographics.init()
    )
    val littersOut = DemographicsTransformations.mapSexSpayNeuter(
      RawRecord(1, litters),
      HlesDogDemographics.init()
    )
    val unknownOut = DemographicsTransformations.mapSexSpayNeuter(
      RawRecord(1, unknown),
      HlesDogDemographics.init()
    )

    noLittersOut.ddSex.value shouldBe 2L
    noLittersOut.ddSpayedOrNeutered.value shouldBe true
    noLittersOut.ddSpayOrNeuterAge.value shouldBe 2L
    noLittersOut.ddSpayMethod.value shouldBe 3L
    noLittersOut.ddHasBeenPregnant.value shouldBe 2L
    noLittersOut.ddLitterCount shouldBe None
    noLittersOut.ddEstrousCycleExperiencedBeforeSpayed.value shouldBe 99L
    noLittersOut.ddEstrousCycleCount shouldBe None

    littersOut.ddSex.value shouldBe 2L
    littersOut.ddSpayedOrNeutered.value shouldBe true
    littersOut.ddSpayOrNeuterAge.value shouldBe 2L
    littersOut.ddSpayMethod.value shouldBe 2L
    littersOut.ddHasBeenPregnant.value shouldBe 1L
    littersOut.ddLitterCount.value shouldBe 3L
    littersOut.ddEstrousCycleExperiencedBeforeSpayed.value shouldBe 2L
    littersOut.ddEstrousCycleCount shouldBe None

    unknownOut.ddSex.value shouldBe 2L
    unknownOut.ddSpayedOrNeutered.value shouldBe true
    unknownOut.ddSpayOrNeuterAge.value shouldBe 2L
    unknownOut.ddSpayMethod.value shouldBe 1L
    unknownOut.ddHasBeenPregnant.value shouldBe 99L
    unknownOut.ddLitterCount shouldBe None
    unknownOut.ddEstrousCycleExperiencedBeforeSpayed.value shouldBe 1L
    unknownOut.ddEstrousCycleCount.value shouldBe 1L
  }

  it should "map sex-related demographics fields for un-spayed female dogs" in {
    val noLitters = Map[String, Array[String]](
      "dd_dog_sex" -> Array("2"),
      "dd_dog_spay_neuter" -> Array("2"),
      "dd_fns_pregnant" -> Array("2"),
      "dd_fns_nbr_cycles" -> Array("10")
    )
    val litters = Map[String, Array[String]](
      "dd_dog_sex" -> Array("2"),
      "dd_dog_spay_neuter" -> Array("2"),
      "dd_fns_pregnant" -> Array("1"),
      "dd_fns_nbr_litters" -> Array("3")
    )
    val unknown = Map[String, Array[String]](
      "dd_dog_sex" -> Array("2"),
      "dd_dog_spay_neuter" -> Array("2"),
      "dd_fns_pregnant" -> Array("99"),
      "dd_fns_nbr_cycles" -> Array("1")
    )

    val noLittersOut = DemographicsTransformations.mapSexSpayNeuter(
      RawRecord(1, noLitters),
      HlesDogDemographics.init()
    )
    val littersOut = DemographicsTransformations.mapSexSpayNeuter(
      RawRecord(1, litters),
      HlesDogDemographics.init()
    )
    val unknownOut = DemographicsTransformations.mapSexSpayNeuter(
      RawRecord(1, unknown),
      HlesDogDemographics.init()
    )

    noLittersOut.ddSex.value shouldBe 2L
    noLittersOut.ddSpayedOrNeutered.value shouldBe false
    noLittersOut.ddHasBeenPregnant.value shouldBe 2L
    noLittersOut.ddLitterCount shouldBe None
    noLittersOut.ddEstrousCycleCount.value shouldBe 10L

    littersOut.ddSex.value shouldBe 2L
    littersOut.ddSpayedOrNeutered.value shouldBe false
    littersOut.ddHasBeenPregnant.value shouldBe 1L
    littersOut.ddLitterCount.value shouldBe 3L
    littersOut.ddEstrousCycleCount shouldBe None

    unknownOut.ddSex.value shouldBe 2L
    unknownOut.ddSpayedOrNeutered.value shouldBe false
    unknownOut.ddHasBeenPregnant.value shouldBe 99L
    unknownOut.ddLitterCount shouldBe None
    unknownOut.ddEstrousCycleCount.value shouldBe 1L
  }

  it should "map weight-related demographics fields" in {
    val example = Map[String, Array[String]](
      "dd_dog_weight" -> Array("3"),
      "dd_dog_weight_lbs" -> Array("100.25"),
      "dd_dog_weight_as_adult" -> Array("4")
    )
    val out = DemographicsTransformations.mapWeight(
      RawRecord(id = 1, example),
      HlesDogDemographics.init()
    )

    out.ddWeightRange.value shouldBe 3L
    out.ddWeightLbs.value shouldBe 100.25d
    out.ddWeightRangeExpectedAdult.value shouldBe 4L
  }

  it should "map insurance-related demographics fields" in {
    val insurance = Map[String, Array[String]](
      "dd_insurance_yn" -> Array("1"),
      "dd_insurance" -> Array("98"),
      "dd_insurance_other" -> Array("My special insurance")
    )
    val noInsurance = Map[String, Array[String]](
      "dd_insurance_yn" -> Array("2"),
      "dd_insurance" -> Array("3")
    )

    val insuranceOut = DemographicsTransformations.mapInsurance(
      RawRecord(1, insurance),
      HlesDogDemographics.init()
    )
    val noInsuranceOut = DemographicsTransformations.mapInsurance(
      RawRecord(1, noInsurance),
      HlesDogDemographics.init()
    )

    insuranceOut.ddInsurance.value shouldBe true
    insuranceOut.ddInsuranceProvider.value shouldBe 98L
    insuranceOut.ddInsuranceProviderOtherDescription.value shouldBe "My special insurance"

    noInsuranceOut.ddInsurance.value shouldBe false
    noInsuranceOut.ddInsuranceProvider shouldBe None
  }

  it should "map acquisition-related demographics fields" in {
    val usBorn = Map[String, Array[String]](
      "dd_us_born" -> Array("1"),
      "dd_acquired_location_yn" -> Array("1"),
      "dd_acquired_st" -> Array("OH"),
      "dd_acquire_source" -> Array("2")
    )
    val international = Map[String, Array[String]](
      "dd_us_born" -> Array("0"),
      "dd_us_born_no" -> Array("UK"),
      "dd_acquire_source" -> Array("5")
    )
    val unknown = Map[String, Array[String]](
      "dd_us_born" -> Array("99"),
      "dd_acquired_location_yn" -> Array("1"),
      "dd_acquired_st" -> Array("OH"),
      "dd_acquire_source" -> Array("98"),
      "dd_acquire_source_other" -> Array("?????")
    )

    val usBornOut = DemographicsTransformations.mapAcquiredInfo(
      RawRecord(1, usBorn),
      HlesDogDemographics.init()
    )
    val internationalOut = DemographicsTransformations.mapAcquiredInfo(
      RawRecord(1, international),
      HlesDogDemographics.init()
    )
    val unknownOut = DemographicsTransformations.mapAcquiredInfo(
      RawRecord(1, unknown),
      HlesDogDemographics.init()
    )

    usBornOut.ddAcquiredCountry.value shouldBe "US"
    usBornOut.ddAcquiredState.value shouldBe "OH"
    usBornOut.ddAcquiredSource.value shouldBe 2L

    internationalOut.ddAcquiredCountry.value shouldBe "UK"
    internationalOut.ddAcquiredState shouldBe None
    internationalOut.ddAcquiredSource.value shouldBe 5L

    unknownOut.ddAcquiredCountry.value shouldBe "99"
    unknownOut.ddAcquiredState.value shouldBe "OH"
    unknownOut.ddAcquiredSource.value shouldBe 98L
    unknownOut.ddAcquiredSourceOtherDescription.value shouldBe "?????"
  }

  it should "map activity-related demographics fields" in {
    val serviceDog = Map[String, Array[String]](
      "dd_activities" -> Array("2", "4", "6", "8", "10", "98"),
      "dd_service_m" -> Array("1"),
      "dd_other_m" -> Array("2"),
      "dd_2nd_activity_other" -> Array("Activity!"),
      "dd_service_type_1" -> Array("1", "3", "5", "7"),
      "dd_service_health_other_1" -> Array("Health?")
    )
    val assistanceDog = Map[String, Array[String]](
      "dd_activities" -> Array("1", "3", "5", "7", "9", "11", "98"),
      "dd_assistance_m" -> Array("2"),
      "dd_other_m" -> Array("1"),
      "dd_1st_activity_other" -> Array("Activity?"),
      "dd_service_type_1" -> Array("2", "4", "6", "98"),
      "dd_service_medical_other_1" -> Array("Medical!"),
      "dd_service_other_1" -> Array("Activity!")
    )

    val serviceOut = DemographicsTransformations.mapActivities(
      RawRecord(1, serviceDog),
      HlesDogDemographics.init()
    )
    val assistanceOut = DemographicsTransformations.mapActivities(
      RawRecord(1, assistanceDog),
      HlesDogDemographics.init()
    )

    serviceOut.ddActivitiesObedience.value shouldBe 3L
    serviceOut.ddActivitiesBreeding.value shouldBe 3L
    serviceOut.ddActivitiesHunting.value shouldBe 3L
    serviceOut.ddActivitiesFieldTrials.value shouldBe 3L
    serviceOut.ddActivitiesService.value shouldBe 1L
    serviceOut.ddActivitiesOther.value shouldBe 2L
    serviceOut.ddActivitiesOtherDescription.value shouldBe "Activity!"
    serviceOut.ddActivitiesServiceSeeingEye.value shouldBe true
    serviceOut.ddActivitiesServiceWheelchair.value shouldBe true
    serviceOut.ddActivitiesServiceOtherHealth.value shouldBe true
    serviceOut.ddActivitiesServiceEmotionalSupport.value shouldBe true
    serviceOut.ddActivitiesServiceOtherHealthDescription.value shouldBe "Health?"

    assistanceOut.ddActivitiesCompanionAnimal.value shouldBe 3L
    assistanceOut.ddActivitiesShow.value shouldBe 3L
    assistanceOut.ddActivitiesAgility.value shouldBe 3L
    assistanceOut.ddActivitiesWorking.value shouldBe 3L
    assistanceOut.ddActivitiesSearchAndRescue.value shouldBe 3L
    assistanceOut.ddActivitiesAssistanceOrTherapy.value shouldBe 2L
    assistanceOut.ddActivitiesOther.value shouldBe 1L
    assistanceOut.ddActivitiesOtherDescription.value shouldBe "Activity?"
    assistanceOut.ddActivitiesServiceHearingOrSignal.value shouldBe true
    assistanceOut.ddActivitiesServiceOtherMedical.value shouldBe true
    assistanceOut.ddActivitiesServiceCommunityTherapy.value shouldBe true
    assistanceOut.ddActivitiesServiceOther.value shouldBe true
    assistanceOut.ddActivitiesServiceOtherMedicalDescription.value shouldBe "Medical!"
    assistanceOut.ddActivitiesServiceOtherDescription.value shouldBe "Activity!"

  }
  it should "map activity-related fallback fields" in {
    val activitiesDog = Map[String, Array[String]](
      "dd_activities" -> Array("3", "5", "7", "9", "11", "98"),
      "dd_assistance_m" -> Array("2"),
      "dd_other_m" -> Array("1"),
      "dd_companion_m" -> Array("1"),
      "dd_1st_activity_other" -> Array("Activity?"),
      "dd_service_type_1" -> Array("2", "4", "6", "98"),
      "dd_service_medical_other_1" -> Array("Medical!"),
      "dd_service_other_1" -> Array("Activity!")
    )

    val activitiesOut = DemographicsTransformations.mapActivities(
      RawRecord(1, activitiesDog),
      HlesDogDemographics.init()
    )

    activitiesOut.ddActivitiesCompanionAnimal.value shouldBe 1L
  }
}
