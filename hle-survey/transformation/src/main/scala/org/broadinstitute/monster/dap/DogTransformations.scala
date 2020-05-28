package org.broadinstitute.monster.dap

import java.time.{LocalDate, Period}

import org.broadinstitute.monster.dogaging.jadeschema.table.HlesDog

object DogTransformations {

  /** Parse all dog-related fields out of a raw RedCap record. */
  def mapDog(rawRecord: RawRecord): HlesDog = {
    val dogBase = HlesDog.init(
      dogId = rawRecord.id,
      ownerId = rawRecord.getRequired("st_owner_id").toLong
    )

    val transformations = List(
      mapStudyStatus _,
      mapBreed _,
      mapAge _,
      mapSexSpayNeuter _,
      mapWeight _,
      mapInsurance _,
      mapAcquiredInfo _,
      mapActivities _,
      mapResidences _
    )

    transformations.foldLeft(dogBase)((acc, f) => f(rawRecord, acc))
  }

  /**
    * Parse all study-status-related fields out of a raw RedCap record,
    * injecting them into a partially-modeled dog record.
    */
  def mapStudyStatus(rawRecord: RawRecord, dog: HlesDog): HlesDog =
    dog.copy(
      stVipOrStaff = rawRecord.getOptionalNumber("st_vip_or_staff"),
      stBatchLabel = rawRecord.getOptional("st_batch_label"),
      stPortalInvitationDate = rawRecord.getOptionalDate("st_invite_to_portal"),
      stPortalAccountCreationDate = rawRecord.getOptionalDate("st_portal_account_date"),
      stHlesCompletionDate = rawRecord.getOptional("st_dap_pack_date").map { timeString =>
        val splitPoint = timeString.indexOf(" ")
        LocalDate.parse(timeString.take(splitPoint))
      }
    )

  /**
    * Parse all breed-related fields out of a raw RedCap record,
    * injecting them into a partially-modeled dog record.
    */
  def mapBreed(rawRecord: RawRecord, dog: HlesDog): HlesDog = {
    val isPureBreed = rawRecord.getBoolean("dd_dog_pure_or_mixed")
    if (isPureBreed) {
      val breed = rawRecord.getOptionalNumber("dd_dog_breed")
      dog.copy(
        ddBreedPure = breed,
        ddBreedPureNonAkc =
          if (breed.contains(277L)) rawRecord.getOptional("dd_dog_breed_non_akc") else None
      )
    } else {
      dog.copy(
        ddBreedMixedPrimary = rawRecord.getOptionalNumber("dd_dog_breed_mix_1"),
        ddBreedMixedSecondary = rawRecord.getOptionalNumber("dd_dog_breed_mix_2")
      )
    }
  }

  /**
    * Parse all age-related fields out of a raw RedCap record,
    * injecting them into a partially-modeled dog record.
    */
  def mapAge(rawRecord: RawRecord, dog: HlesDog): HlesDog = {
    val ageCertain = rawRecord.getBoolean("dd_dog_birth_year_certain")

    /*
     * FIXME: How do we encode this lookup table in the data?
     *
     * Age basis codebook:
     *  - 1 == Calculated from birth year and month
     *  - 2 == Estimated from birth year
     *  - 3 == Estimated by owner
     */

    if (ageCertain) {
      val formYear = rawRecord.getRequired("dd_dog_current_year_calc").toInt
      val birthYear = rawRecord.getRequired("dd_dog_birth_year").toInt

      val formMonth = rawRecord.getRequired("dd_dog_current_month_calc").toInt
      val exactMonthKnown = rawRecord.getBoolean("dd_dog_birth_month_yn")
      val birthMonth = if (exactMonthKnown) {
        rawRecord.getRequired("dd_dog_birth_month").toInt
      } else {
        // Use the midpoint of the birth year to estimate the age.
        // Mimics the behavior DAP said to use for estimating day-of-month.
        // ¯\_(ツ)_/¯
        6
      }

      val age = {
        val formDate = LocalDate.of(formYear, formMonth, 15)
        val birthDate = LocalDate.of(birthYear, birthMonth, 15)
        val age = Period.between(birthDate, formDate)
        age.getYears.toDouble + age.getMonths.toDouble / 12
      }

      val sources = rawRecord.getArray("dd_dog_age_certain_why")

      dog.copy(
        ddAgeYears = Some(age),
        ddAgeBasis = Some(if (exactMonthKnown) 1 else 2),
        ddAgeExactSourceAcquiredAsPuppy = Some(sources.contains("1")),
        ddAgeExactSourceRegistrationInformation = Some(sources.contains("2")),
        ddAgeExactSourceDeterminedByRescueOrg = Some(sources.contains("3")),
        ddAgeExactSourceDeterminedByVeterinarian = Some(sources.contains("4")),
        ddAgeExactSourceFromLitterOwnerBred = Some(sources.contains("5")),
        ddAgeExactSourceOther = Some(sources.contains("98")),
        ddAgeExactSourceOtherDescription =
          if (sources.contains("98")) rawRecord.getOptional("dd_dog_age_certain_other") else None,
        ddBirthYear = Some(birthYear.toLong),
        ddBirthMonth = if (exactMonthKnown) Some(birthMonth.toLong) else None
      )
    } else {
      val sources = rawRecord.getArray("dd_dog_age_estimate_why")

      dog.copy(
        ddAgeYears = rawRecord.getOptional("dd_dog_age").map(_.toDouble),
        ddAgeBasis = Some(3),
        ddAgeEstimateSourceToldByPreviousOwner = Some(sources.contains("1")),
        // Not a typo, not sure what happened to option 2.
        ddAgeEstimateSourceDeterminedByRescueOrg = Some(sources.contains("3")),
        ddAgeEstimateSourceDeterminedByVeterinarian = Some(sources.contains("4")),
        ddAgeEstimateSourceOther = Some(sources.contains("98")),
        ddAgeEstimateSourceOtherDescription =
          if (sources.contains("98")) rawRecord.getOptional("dd_dog_age_estimate_other") else None
      )
    }
  }

  /**
    * Parse all sex-related fields out of a raw RedCap record,
    * injecting them into a partially-modeled dog record.
    */
  def mapSexSpayNeuter(rawRecord: RawRecord, dog: HlesDog): HlesDog = {
    val sex = rawRecord.getOptionalNumber("dd_dog_sex")
    val spayedOrNeutered = rawRecord.getBoolean("dd_dog_spay_neuter")
    val spayOrNeuterAge = if (spayedOrNeutered) {
      rawRecord.getOptionalNumber("dd_spay_or_neuter_age")
    } else {
      None
    }
    val male = sex.contains(1L)

    if (male) {
      val siredLitters = if (spayedOrNeutered) {
        rawRecord.getOptionalNumber("dd_ms_sired_yn")
      } else {
        rawRecord.getOptionalNumber("dd_mns_sired_yn")
      }
      val litterCountField =
        if (spayedOrNeutered) "dd_mns_nbr_litters_2" else "dd_mns_nbr_litters"
      dog.copy(
        ddSex = sex,
        ddSpayedOrNeutered = Some(spayedOrNeutered),
        ddSpayOrNeuterAge = spayOrNeuterAge,
        ddHasSiredLitters = siredLitters,
        ddLitterCount =
          if (siredLitters.contains(1)) rawRecord.getOptionalNumber(litterCountField) else None
      )
    } else {
      val abbrev = if (spayedOrNeutered) "fs" else "fns"
      val pregnant = rawRecord.getOptionalNumber(s"dd_${abbrev}_pregnant")
      dog.copy(
        ddSex = sex,
        ddSpayedOrNeutered = Some(spayedOrNeutered),
        ddSpayOrNeuterAge = spayOrNeuterAge,
        ddSpayMethod =
          if (spayedOrNeutered) rawRecord.getOptionalNumber("dd_fs_spay_method") else None,
        ddEstrousCycleExperiencedBeforeSpayed =
          if (spayedOrNeutered) rawRecord.getOptionalNumber("dd_fs_heat_yn") else None,
        ddEstrousCycleCount = rawRecord.getOptionalNumber(s"dd_${abbrev}_nbr_cycles"),
        ddHasBeenPregnant = pregnant,
        ddLitterCount =
          if (pregnant.contains(1)) rawRecord.getOptionalNumber(s"dd_${abbrev}_nbr_litters")
          else None
      )
    }
  }

  /**
    * Parse all weight-related fields out of a raw RedCap record,
    * injecting them into a partially-modeled dog record.
    */
  def mapWeight(rawRecord: RawRecord, dog: HlesDog): HlesDog = dog.copy(
    ddWeightRange = rawRecord.getOptionalNumber("dd_dog_weight"),
    ddWeightLbs = rawRecord.getOptional("dd_dog_weight_lbs").map(_.toDouble),
    ddWeightRangeExpectedAdult = rawRecord.getOptionalNumber("dd_weight_range_expected_adult")
  )

  /**
    * Parse all insurance-related fields out of a raw RedCap record,
    * injecting them into a partially-modeled dog record.
    */
  def mapInsurance(rawRecord: RawRecord, dog: HlesDog): HlesDog =
    if (rawRecord.getBoolean("dd_insurance_yn")) {
      val provider = rawRecord.getOptionalNumber("dd_insurance")
      dog.copy(
        ddInsuranceProvider = provider,
        ddInsuranceProviderOtherDescription =
          if (provider.contains(98)) rawRecord.getOptional("dd_insurance_other") else None
      )
    } else {
      dog
    }

  /**
    * Parse all acquisition-related fields out of a raw RedCap record,
    * injecting them into a partially-modeled dog record.
    */
  def mapAcquiredInfo(rawRecord: RawRecord, dog: HlesDog): HlesDog = {
    val usBorn = rawRecord.getRequired("dd_us_born")
    val country = usBorn match {
      case "1"   => Some("US")
      case "0"   => rawRecord.getOptional("dd_us_born_no")
      case other => Some(other)
    }
    val source = rawRecord.getOptionalNumber("dd_acquire_source")
    val locationKnown = rawRecord.getBoolean("dd_acquired_location_yn")

    dog.copy(
      ddAcquiredYear = rawRecord.getOptionalNumber("dd_acquire_year"),
      ddAcquiredMonth = rawRecord.getOptionalNumber("dd_acquire_month"),
      ddAcquiredSource = source,
      ddAcquiredSourceOtherDescription =
        if (source.contains(98)) rawRecord.getOptional("dd_acquire_source_other") else None,
      ddAcquiredCountry = country,
      ddAcquiredState = if (locationKnown) rawRecord.getOptional("dd_acquired_st") else None,
      ddAcquiredZip = if (locationKnown) rawRecord.getOptional("dd_acquired_zip") else None
    )
  }

  /**
    * Map from activity string labels to their corresponding raw values.
    *
    * Very unfortunate that we have to hard-code this.
    */
  val ActivityValues = Map(
    "companion" -> 1L,
    "obedience" -> 2L,
    "show" -> 3L,
    "breeding" -> 4L,
    "agility" -> 5L,
    "hunting" -> 6L,
    "working" -> 7L,
    "field" -> 8L,
    "search_rescue" -> 9L,
    "service" -> 10L,
    "assistance" -> 11L,
    "other" -> 98L
  )

  /**
    * Parse all activity-related fields out of a raw RedCap record,
    * injecting them into a partially-modeled dog record.
    */
  def mapActivities(rawRecord: RawRecord, dog: HlesDog): HlesDog = {
    val allActivities = rawRecord.getArray("dd_activities").map(_.toLong)

    def activityLevel(activity: String): Option[Long] =
      if (allActivities.contains[Long](ActivityValues(activity))) {
        rawRecord.getOptionalNumber(s"dd_${activity}_m").orElse(Some(3L))
      } else {
        None
      }

    val serviceLevel = activityLevel("service")
    val assistanceLevel = activityLevel("assistance")
    val otherLevel = activityLevel("other")

    val serviceTypes = if (serviceLevel.exists(_ != 3L) || assistanceLevel.exists(_ != 3L)) {
      Some(rawRecord.getArray("dd_service_type_1"))
    } else {
      None
    }
    val otherMedService = serviceTypes.map(_.contains("4"))
    val otherHealthService = serviceTypes.map(_.contains("5"))
    val otherService = serviceTypes.map(_.contains("98"))

    dog.copy(
      ddActivitiesCompanionAnimal = activityLevel("companion"),
      ddActivitiesObedience = activityLevel("obedience"),
      ddActivitiesShow = activityLevel("show"),
      ddActivitiesBreeding = activityLevel("breeding"),
      ddActivitiesAgility = activityLevel("agility"),
      ddActivitiesHunting = activityLevel("hunting"),
      ddActivitiesWorking = activityLevel("working"),
      ddActivitiesFieldTrials = activityLevel("field"),
      ddActivitiesSearchAndRescue = activityLevel("search_rescue"),
      ddActivitiesService = serviceLevel,
      ddActivitiesAssistanceOrTherapy = assistanceLevel,
      ddActivitiesOther = otherLevel,
      ddActivitiesOtherDescription = otherLevel.flatMap {
        case 1L => rawRecord.getOptional("dd_1st_activity_other")
        case 2L => rawRecord.getOptional("dd_2nd_activity_other")
        case _  => None
      },
      ddActivitiesServiceSeeingEye = serviceTypes.map(_.contains("1")),
      ddActivitiesServiceHearingOrSignal = serviceTypes.map(_.contains("1")),
      ddActivitiesServiceWheelchair = serviceTypes.map(_.contains("1")),
      ddActivitiesServiceOtherMedical = otherMedService,
      ddActivitiesServiceOtherMedicalDescription =
        if (otherMedService.contains(true)) rawRecord.getOptional("dd_service_medical_other_1")
        else None,
      ddActivitiesServiceOtherHealth = otherHealthService,
      ddActivitiesServiceOtherHealthDescription =
        if (otherHealthService.contains(true)) rawRecord.getOptional("dd_service_health_other_1")
        else None,
      ddActivitiesServiceCommunityTherapy = serviceTypes.map(_.contains("1")),
      ddActivitiesServiceEmotionalSupport = serviceTypes.map(_.contains("1")),
      ddActivitiesServiceOther = otherService,
      ddActivitiesServiceOtherDescription =
        if (otherService.contains(true)) rawRecord.getOptional("dd_service_other_1") else None
    )
  }

  /**
    * Parse all residence-related fields out of a raw RedCap record,
    * injecting them into a partially-modeled dog record.
    */
  def mapResidences(rawRecord: RawRecord, dog: HlesDog): HlesDog = {
    val hasSecondaryResidence = rawRecord.getBoolean("oc_address2_yn")
    val hasTertiaryResidences = rawRecord.getBoolean("dd_2nd_residence_yn")
    val tertiaryResidenceCount = if (hasTertiaryResidences) {
      rawRecord.getRequired("dd_2nd_residence_nbr").toInt
    } else {
      0
    }

    val tertiaryResidences = List.tabulate(tertiaryResidenceCount) { i =>
      val prefix = f"dd_2nd_residence_${i + 1}%02d"
      val state = rawRecord.getOptional(s"${prefix}_st")
      val zip = rawRecord.getOptional(s"${prefix}_zip")
      val weeks = rawRecord.getOptionalNumber(s"${prefix}_time")
      (state, zip, weeks)
    }

    val primaryOwned = rawRecord.getOptionalNumber("oc_address1_own")
    val secondaryOwned =
      if (hasSecondaryResidence) rawRecord.getOptionalNumber("oc_address2_own") else None

    dog.copy(
      ocPrimaryResidenceState = rawRecord.getOptional("oc_address1_state"),
      ocPrimaryResidenceCensusDivision = rawRecord.getOptionalNumber("oc_address1_division"),
      ocPrimaryResidenceZip = rawRecord.getOptional("oc_address1_zip"),
      ocPrimaryResidenceOwnership = primaryOwned,
      ocPrimaryResidenceOwnershipOtherDescription =
        if (primaryOwned.contains(98)) rawRecord.getOptional("oc_address1_own_other") else None,
      ocPrimaryResidenceTimePercentage =
        if (hasSecondaryResidence) rawRecord.getOptionalNumber("oc_address1_pct") else None,
      ocSecondaryResidenceState =
        if (hasSecondaryResidence) rawRecord.getOptional("oc_address2_state") else None,
      ocSecondaryResidenceZip = if (hasSecondaryResidence) {
        rawRecord.getOptional("oc_address2_zip")
      } else {
        None
      },
      ocSecondaryResidenceOwnership = secondaryOwned,
      ocSecondaryResidenceOwnershipOtherDescription = if (secondaryOwned.contains(98)) {
        rawRecord.getOptional("oc_address2_own_other")
      } else {
        None
      },
      ocSecondaryResidenceTimePercentage = if (hasSecondaryResidence) {
        rawRecord.getOptionalNumber("oc_2nd_address_pct")
      } else {
        None
      },
      ddAlternateRecentResidenceCount = Some(tertiaryResidenceCount.toLong),
      ddAlternateRecentResidence1State =
        if (tertiaryResidenceCount >= 1) tertiaryResidences(0)._1 else None,
      ddAlternateRecentResidence1Zip =
        if (tertiaryResidenceCount >= 1) tertiaryResidences(0)._2 else None,
      ddAlternateRecentResidence1Weeks =
        if (tertiaryResidenceCount >= 1) tertiaryResidences(0)._3 else None,
      ddAlternateRecentResidence2State =
        if (tertiaryResidenceCount >= 2) tertiaryResidences(1)._1 else None,
      ddAlternateRecentResidence2Zip =
        if (tertiaryResidenceCount >= 2) tertiaryResidences(1)._2 else None,
      ddAlternateRecentResidence2Weeks =
        if (tertiaryResidenceCount >= 2) tertiaryResidences(1)._3 else None,
      ddAlternateRecentResidence3State =
        if (tertiaryResidenceCount >= 3) tertiaryResidences(2)._1 else None,
      ddAlternateRecentResidence3Zip =
        if (tertiaryResidenceCount >= 3) tertiaryResidences(2)._2 else None,
      ddAlternateRecentResidence3Weeks =
        if (tertiaryResidenceCount >= 3) tertiaryResidences(2)._3 else None,
      ddAlternateRecentResidence4State =
        if (tertiaryResidenceCount >= 4) tertiaryResidences(3)._1 else None,
      ddAlternateRecentResidence4Zip =
        if (tertiaryResidenceCount >= 4) tertiaryResidences(3)._2 else None,
      ddAlternateRecentResidence4Weeks =
        if (tertiaryResidenceCount >= 4) tertiaryResidences(3)._3 else None,
      ddAlternateRecentResidence5State =
        if (tertiaryResidenceCount >= 5) tertiaryResidences(4)._1 else None,
      ddAlternateRecentResidence5Zip =
        if (tertiaryResidenceCount >= 5) tertiaryResidences(4)._2 else None,
      ddAlternateRecentResidence5Weeks =
        if (tertiaryResidenceCount >= 5) tertiaryResidences(4)._3 else None,
      ddAlternateRecentResidence6State =
        if (tertiaryResidenceCount >= 6) tertiaryResidences(5)._1 else None,
      ddAlternateRecentResidence6Zip =
        if (tertiaryResidenceCount >= 6) tertiaryResidences(5)._2 else None,
      ddAlternateRecentResidence6Weeks =
        if (tertiaryResidenceCount >= 6) tertiaryResidences(5)._3 else None,
      ddAlternateRecentResidence7State =
        if (tertiaryResidenceCount >= 7) tertiaryResidences(6)._1 else None,
      ddAlternateRecentResidence7Zip =
        if (tertiaryResidenceCount >= 7) tertiaryResidences(6)._2 else None,
      ddAlternateRecentResidence7Weeks =
        if (tertiaryResidenceCount >= 7) tertiaryResidences(6)._3 else None,
      ddAlternateRecentResidence8State =
        if (tertiaryResidenceCount >= 8) tertiaryResidences(7)._1 else None,
      ddAlternateRecentResidence8Zip =
        if (tertiaryResidenceCount >= 8) tertiaryResidences(7)._2 else None,
      ddAlternateRecentResidence8Weeks =
        if (tertiaryResidenceCount >= 8) tertiaryResidences(7)._3 else None,
      ddAlternateRecentResidence9State =
        if (tertiaryResidenceCount >= 9) tertiaryResidences(8)._1 else None,
      ddAlternateRecentResidence9Zip =
        if (tertiaryResidenceCount >= 9) tertiaryResidences(8)._2 else None,
      ddAlternateRecentResidence9Weeks =
        if (tertiaryResidenceCount >= 9) tertiaryResidences(8)._3 else None,
      ddAlternateRecentResidence10State =
        if (tertiaryResidenceCount >= 10) tertiaryResidences(9)._1 else None,
      ddAlternateRecentResidence10Zip =
        if (tertiaryResidenceCount >= 10) tertiaryResidences(9)._2 else None,
      ddAlternateRecentResidence10Weeks =
        if (tertiaryResidenceCount >= 10) tertiaryResidences(9)._3 else None,
      ddAlternateRecentResidence11State =
        if (tertiaryResidenceCount >= 11) tertiaryResidences(10)._1 else None,
      ddAlternateRecentResidence11Zip =
        if (tertiaryResidenceCount >= 11) tertiaryResidences(10)._2 else None,
      ddAlternateRecentResidence11Weeks =
        if (tertiaryResidenceCount >= 11) tertiaryResidences(10)._3 else None,
      ddAlternateRecentResidence12State =
        if (tertiaryResidenceCount >= 12) tertiaryResidences(11)._1 else None,
      ddAlternateRecentResidence12Zip =
        if (tertiaryResidenceCount >= 12) tertiaryResidences(11)._2 else None,
      ddAlternateRecentResidence12Weeks =
        if (tertiaryResidenceCount >= 12) tertiaryResidences(11)._3 else None,
      ddAlternateRecentResidence13State =
        if (tertiaryResidenceCount >= 13) tertiaryResidences(12)._1 else None,
      ddAlternateRecentResidence13Zip =
        if (tertiaryResidenceCount >= 13) tertiaryResidences(12)._2 else None,
      ddAlternateRecentResidence13Weeks =
        if (tertiaryResidenceCount >= 13) tertiaryResidences(12)._3 else None
    )
  }
}
