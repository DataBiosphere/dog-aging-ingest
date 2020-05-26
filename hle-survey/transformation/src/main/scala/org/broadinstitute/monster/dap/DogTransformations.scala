package org.broadinstitute.monster.dap

import java.time.{LocalDate, Period}

import org.broadinstitute.monster.dogaging.jadeschema.struct.HlesDogResidence
import org.broadinstitute.monster.dogaging.jadeschema.table.HlesDog

object DogTransformations {

  /** Parse all dog-related fields out of a raw RedCap record. */
  def mapDog(rawRecord: RawRecord): HlesDog = {
    val dogBase = HlesDog.init(
      dogId = rawRecord.id,
      // FIXME: Once DAP figures out a name for a dedicated owner ID, use that.
      ownerId = rawRecord.id
    )

    val transformations = List(
      mapStudyStatus _,
      mapBreed _,
      mapAge _,
      mapSexSpayNeuter _,
      mapWeight _,
      mapInsurance _,
      mapAcquiredInfo _,
      mapRoles _,
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
      stHlesCompletionDate = rawRecord.getOptionalDate("st_dap_pack_date")
    )

  /**
    * Parse all breed-related fields out of a raw RedCap record,
    * injecting them into a partially-modeled dog record.
    */
  def mapBreed(rawRecord: RawRecord, dog: HlesDog): HlesDog = {
    val isPureBreed = rawRecord.getBoolean("dd_dog_pure_or_mixed")
    if (isPureBreed) {
      val breed = rawRecord.getOptional("dd_dog_breed")
      dog.copy(
        ddBreedPure = breed,
        ddBreedPureNonAkc =
          if (breed.contains("277")) rawRecord.getOptional("dd_dog_breed_non_akc") else None
      )
    } else {
      dog.copy(
        ddBreedMixedPrimary = rawRecord.getOptional("dd_dog_breed_mix_1"),
        ddBreedMixedSecondary = rawRecord.getOptional("dd_dog_breed_mix_2")
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
     *  - '1' == Calculated from birth year and month
     *  - '2' == Estimated from birth year
     *  - '3' == Estimated by owner
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

      val source = rawRecord.getArray("dd_dog_age_certain_why")

      dog.copy(
        ddAgeYears = Some(age),
        ddAgeBasis = Some(if (exactMonthKnown) "1" else "2"),
        ddAgeExactSource = source,
        ddAgeExactSourceOther =
          if (source.contains("98")) rawRecord.getOptional("dd_dog_age_certain_other") else None,
        ddBirthYear = Some(birthYear.toString),
        ddBirthMonth = if (exactMonthKnown) Some(birthMonth.toString) else None
      )
    } else {
      val source = rawRecord.getArray("dd_dog_age_estimate_why")

      dog.copy(
        ddAgeYears = rawRecord.getOptional("dd_dog_age").map(_.toDouble),
        ddAgeBasis = Some("3"),
        ddAgeEstimateSource = source,
        ddAgeEstimateSourceOther =
          if (source.contains("98")) rawRecord.getOptional("dd_dog_age_estimate_other") else None
      )
    }
  }

  /**
    * Parse all sex-related fields out of a raw RedCap record,
    * injecting them into a partially-modeled dog record.
    */
  def mapSexSpayNeuter(rawRecord: RawRecord, dog: HlesDog): HlesDog = {
    val sex = rawRecord.getOptional("dd_dog_sex")
    val spayedOrNeutered = rawRecord.getBoolean("dd_dog_spay_neuter")
    val spayOrNeuterAge = if (spayedOrNeutered) {
      rawRecord.getOptional("dd_spay_or_neuter_age")
    } else {
      None
    }
    val male = sex.contains("1")

    if (male) {
      val siredLitters = if (spayedOrNeutered) {
        rawRecord.getOptional("dd_ms_sired_yn")
      } else {
        rawRecord.getOptional("dd_mns_sired_yn")
      }
      val litterCountField =
        if (spayedOrNeutered) "dd_mns_nbr_litters_2" else "dd_mns_nbr_litters"
      dog.copy(
        ddSex = sex,
        ddSpayedOrNeutered = Some(spayedOrNeutered),
        ddSpayOrNeuterAge = spayOrNeuterAge,
        ddHasSiredLitters = siredLitters,
        ddLitterCount =
          if (siredLitters.contains("1")) rawRecord.getOptional(litterCountField) else None
      )
    } else {
      val abbrev = if (spayedOrNeutered) "fs" else "fns"
      val pregnant = rawRecord.getOptional(s"dd_${abbrev}_pregnant")
      dog.copy(
        ddSex = sex,
        ddSpayedOrNeutered = Some(spayedOrNeutered),
        ddSpayOrNeuterAge = spayOrNeuterAge,
        ddSpayMethod = if (spayedOrNeutered) rawRecord.getOptional("dd_fs_spay_method") else None,
        ddEstrousCycleExperiencedBeforeSpayed =
          if (spayedOrNeutered) rawRecord.getOptional("dd_fs_heat_yn") else None,
        ddEstrousCycleCount = rawRecord.getOptional(s"dd_${abbrev}_nbr_cycles"),
        ddHasBeenPregnant = pregnant,
        ddLitterCount =
          if (pregnant.contains("1")) rawRecord.getOptional(s"dd_${abbrev}_nbr_litters") else None
      )
    }
  }

  /**
    * Parse all weight-related fields out of a raw RedCap record,
    * injecting them into a partially-modeled dog record.
    */
  def mapWeight(rawRecord: RawRecord, dog: HlesDog): HlesDog = dog.copy(
    ddWeightRange = rawRecord.getOptional("dd_dog_weight"),
    ddWeightLbs = rawRecord.getOptional("dd_dog_weight_lbs").map(_.toDouble),
    ddWeightRangeExpectedAdult = rawRecord.getOptional("dd_weight_range_expected_adult")
  )

  /**
    * Parse all insurance-related fields out of a raw RedCap record,
    * injecting them into a partially-modeled dog record.
    */
  def mapInsurance(rawRecord: RawRecord, dog: HlesDog): HlesDog =
    if (rawRecord.getBoolean("dd_insurance_yn")) {
      val provider = rawRecord.getOptional("dd_insurance")
      dog.copy(
        ddInsuranceProvider = provider,
        ddInsuranceProviderOther =
          if (provider.contains("98")) rawRecord.getOptional("dd_insurance_other") else None
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
    val source = rawRecord.getOptional("dd_acquire_source")
    val locationKnown = rawRecord.getBoolean("dd_acquired_location_yn")

    dog.copy(
      ddAcquiredYear = rawRecord.getOptional("dd_acquire_year"),
      ddAcquiredMonth = rawRecord.getOptional("dd_acquire_month"),
      ddAcquiredSource = source,
      ddAcquiredSourceOther =
        if (source.contains("98")) rawRecord.getOptional("dd_acquire_source_other") else None,
      ddAcquiredCountry = country,
      ddAcquiredState = if (locationKnown) rawRecord.getOptional("dd_acquired_st") else None,
      ddAcquiredZip = if (locationKnown) rawRecord.getOptional("dd_acquired_zip") else None
    )
  }

  /**
    * Map from 'raw' value for every dog role to its corresponding
    * string label as used in other attributes.
    *
    * Very unfortunate that we have to hard-code this.
    */
  val RoleToLabel = Map(
    "1" -> "companion",
    "2" -> "obedience",
    "3" -> "show",
    "4" -> "breeding",
    "5" -> "agility",
    "6" -> "hunting",
    "7" -> "working",
    "8" -> "field",
    "9" -> "search_rescue",
    "10" -> "service",
    "11" -> "assistance",
    "98" -> "other"
  )

  /**
    * Parse all role-related fields out of a raw RedCap record,
    * injecting them into a partially-modeled dog record.
    */
  def mapRoles(rawRecord: RawRecord, dog: HlesDog): HlesDog = {
    val allRoles = rawRecord.getArray("dd_activities")
    val (primary, secondary, other) =
      allRoles.foldLeft((Option.empty[String], Option.empty[String], List.empty[String])) {
        case ((primaryAcc, secondaryAcc, otherAcc), role) =>
          val roleLabel = RoleToLabel(role)
          rawRecord.getOptional(s"dd_${roleLabel}_m") match {
            case Some("1") => (Some(role), secondaryAcc, otherAcc)
            case Some(_)   => (primaryAcc, Some(role), otherAcc)
            case None      => (primaryAcc, secondaryAcc, role :: otherAcc)
          }
      }

    val serviceDog =
      primary.contains("10") || secondary.contains("10") || other.contains("10") ||
        primary.contains("11") || secondary.contains("11") || other.contains("11")
    val serviceTypes =
      if (serviceDog) rawRecord.getArray("dd_service_type_1") else Array.empty[String]

    dog.copy(
      ddPrimaryRole = primary,
      ddPrimaryRoleOther =
        if (primary.contains("98")) rawRecord.getOptional("dd_1st_activity_other") else None,
      ddSecondaryRole = secondary,
      ddSecondaryRoleOther =
        if (secondary.contains("98")) rawRecord.getOptional("dd_2nd_activity_other") else None,
      ddOtherRoles = other.reverse.toArray,
      ddServiceTypes = serviceTypes,
      ddServiceTypesOther =
        if (serviceTypes.contains("98")) rawRecord.getOptional("dd_service_other_1") else None,
      ddServiceTypesOtherHealth = if (serviceTypes.contains("5")) {
        rawRecord.getOptional("dd_service_health_other_1")
      } else {
        None
      },
      ddServiceTypesOtherMedical = if (serviceTypes.contains("4")) {
        rawRecord.getOptional("dd_service_medical_other_1")
      } else {
        None
      }
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
    val tertiaryResidences = Array.tabulate(tertiaryResidenceCount) { i =>
      val prefix = f"dd_2nd_residence_${i + 1}%02d"
      val state = rawRecord.getOptional(s"${prefix}_st")
      val weeks = rawRecord.getOptional(s"${prefix}_time").map(_.toLong)
      HlesDogResidence(state, weeks)
    }

    val primaryOwned = rawRecord.getOptional("oc_address1_own")
    val secondaryOwned =
      if (hasSecondaryResidence) rawRecord.getOptional("oc_address2_own") else None

    dog.copy(
      ocPrimaryResidenceState = rawRecord.getOptional("oc_address1_state"),
      ocPrimaryResidenceCensusDivision = rawRecord.getOptional("oc_address1_division"),
      ocPrimaryResidenceZip = rawRecord.getOptional("oc_address1_zip"),
      ocPrimaryResidenceOwnership = primaryOwned,
      ocPrimaryResidenceOwnershipOther =
        if (primaryOwned.contains("98")) rawRecord.getOptional("oc_address1_own_other") else None,
      ocPrimaryResidenceTimePercentage =
        if (hasSecondaryResidence) rawRecord.getOptional("oc_address1_pct") else None,
      ocSecondaryResidenceState =
        if (hasSecondaryResidence) rawRecord.getOptional("oc_address2_state") else None,
      ocSecondaryResidenceZip = if (hasSecondaryResidence) {
        rawRecord.getOptional("oc_address2_zip")
      } else {
        None
      },
      ocSecondaryResidenceOwnership = secondaryOwned,
      ocSecondaryResidenceOwnershipOther = if (secondaryOwned.contains("98")) {
        rawRecord.getOptional("oc_address2_own_other")
      } else {
        None
      },
      ocSecondaryResidenceTimePercentage = if (hasSecondaryResidence) {
        rawRecord.getOptional("oc_2nd_address_pct")
      } else {
        None
      },
      ddOtherRecentShortTermResidences = tertiaryResidences
    )
  }
}