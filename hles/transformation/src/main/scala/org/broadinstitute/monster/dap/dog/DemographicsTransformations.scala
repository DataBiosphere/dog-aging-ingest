package org.broadinstitute.monster.dap.dog

import java.time.{LocalDate, Period}

import org.broadinstitute.monster.dap.{MissingCalcFieldError, RawRecord}
import org.broadinstitute.monster.dogaging.jadeschema.fragment.HlesDogDemographics

object DemographicsTransformations {

  /** Map all demographics-related fields out of a raw RedCap record into a partial Dog model. */
  def mapDemographics(rawRecord: RawRecord): HlesDogDemographics = {
    val init = HlesDogDemographics.init()

    val transformations = List(
      mapBreed _,
      mapAge _,
      mapSexSpayNeuter _,
      mapWeight _,
      mapInsurance _,
      mapAcquiredInfo _,
      mapActivities _
    )

    transformations.foldLeft(init)((acc, f) => f(rawRecord, acc))
  }

  /**
    * Parse all breed-related fields out of a raw RedCap record,
    * injecting them into a partially-modeled dog record.
    */
  def mapBreed(rawRecord: RawRecord, dog: HlesDogDemographics): HlesDogDemographics = {
    val breedType = rawRecord.getOptionalNumber("dd_dog_pure_or_mixed")
    breedType match {
      case Some(1) =>
        val breed = rawRecord.getOptionalNumber("dd_dog_breed")
        dog.copy(
          ddBreedPureOrMixed = breedType,
          ddBreedPure = breed,
          ddBreedPureNonAkc =
            if (breed.contains(277L)) rawRecord.getOptionalStripped("dd_dog_breed_non_akc")
            else None
        )
      case Some(2) =>
        dog.copy(
          ddBreedPureOrMixed = breedType,
          ddBreedMixedPrimary = rawRecord.getOptionalNumber("dd_dog_breed_mix_1"),
          ddBreedMixedSecondary = rawRecord.getOptionalNumber("dd_dog_breed_mix_2")
        )
      case _ => dog
    }
  }

  /**
    * Parse all age-related fields out of a raw RedCap record,
    * injecting them into a partially-modeled dog record.
    */
  def mapAge(rawRecord: RawRecord, dog: HlesDogDemographics): HlesDogDemographics =
    rawRecord.getOptionalBoolean("dd_dog_birth_year_certain").fold(dog) { ageCertain =>
      if (ageCertain) {
        val dogId = rawRecord.id
        val formYear = rawRecord.getOptional("dd_dog_current_year_calc") match {
          case Some(year) => year.toInt
          case None       =>
            // we're throwing these errors instead of logging them because we want to completely skip records missing this value. they get logged
            // by the logic up the chain that catches the error.
            throw MissingCalcFieldError(
              s"Record $dogId has less than 1 value for field dd_dog_current_year_calc"
            )
        }

        val birthYear = rawRecord.getRequired("dd_dog_birth_year").toInt

        val formMonth = rawRecord.getOptional("dd_dog_current_month_calc") match {
          case Some(month) => month.toInt
          case None        =>
            // we're throwing these errors instead of logging them because we want to completely skip records missing this value. they get logged
            // by the logic up the chain that catches the error.
            throw MissingCalcFieldError(
              s"Record $dogId has less than 1 value for field dd_dog_current_month_calc"
            )
        }
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
          ddAgeBasis = Some(if (exactMonthKnown) AgeBasis.Calculated else AgeBasis.EstimatedFromYear)
            .map(_.value),
          ddAgeExactSourceAcquiredAsPuppy = Some(sources.contains("1")),
          ddAgeExactSourceRegistrationInformation = Some(sources.contains("2")),
          ddAgeExactSourceDeterminedByRescueOrg = Some(sources.contains("3")),
          ddAgeExactSourceDeterminedByVeterinarian = Some(sources.contains("4")),
          ddAgeExactSourceFromLitterOwnerBred = Some(sources.contains("5")),
          ddAgeExactSourceOther = Some(sources.contains("98")),
          ddAgeExactSourceOtherDescription =
            if (sources.contains("98")) rawRecord.getOptionalStripped("dd_dog_age_certain_other")
            else None,
          ddBirthYear = Some(birthYear.toLong),
          ddBirthMonthKnown = Some(exactMonthKnown),
          ddBirthMonth = if (exactMonthKnown) Some(birthMonth.toLong) else None
        )
      } else {
        val sources = rawRecord.getArray("dd_dog_age_estimate_why")

        dog.copy(
          ddAgeYears = rawRecord.getOptional("dd_dog_age").map(_.toDouble),
          ddAgeBasis = Some(AgeBasis.EstimatedByOwner.value),
          ddAgeEstimateSourceToldByPreviousOwner = Some(sources.contains("1")),
          // Not a typo, not sure what happened to option 2.
          ddAgeEstimateSourceDeterminedByRescueOrg = Some(sources.contains("3")),
          ddAgeEstimateSourceDeterminedByVeterinarian = Some(sources.contains("4")),
          ddAgeEstimateSourceOther = Some(sources.contains("98")),
          ddAgeEstimateSourceOtherDescription =
            if (sources.contains("98")) rawRecord.getOptionalStripped("dd_dog_age_estimate_other")
            else None
        )
      }
    }

  /**
    * Parse all sex-related fields out of a raw RedCap record,
    * injecting them into a partially-modeled dog record.
    */
  def mapSexSpayNeuter(rawRecord: RawRecord, dog: HlesDogDemographics): HlesDogDemographics = {
    val sex = rawRecord.getOptionalNumber("dd_dog_sex")
    rawRecord.getOptionalBoolean("dd_dog_spay_neuter").fold(dog.copy(ddSex = sex)) {
      spayedOrNeutered =>
        val spayOrNeuterAge = if (spayedOrNeutered) {
          rawRecord.getOptionalNumber("dd_spay_or_neuter_age")
        } else {
          None
        }

        if (sex.contains(1L)) {
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
  }

  /**
    * Parse all weight-related fields out of a raw RedCap record,
    * injecting them into a partially-modeled dog record.
    */
  def mapWeight(rawRecord: RawRecord, dog: HlesDogDemographics): HlesDogDemographics =
    dog.copy(
      ddWeightRange = rawRecord.getOptionalNumber("dd_dog_weight"),
      ddWeightLbs = rawRecord.getOptional("dd_dog_weight_lbs").map(_.toDouble),
      ddWeightRangeExpectedAdult = rawRecord.getOptionalNumber("dd_dog_weight_as_adult")
    )

  /**
    * Parse all insurance-related fields out of a raw RedCap record,
    * injecting them into a partially-modeled dog record.
    */
  def mapInsurance(rawRecord: RawRecord, dog: HlesDogDemographics): HlesDogDemographics =
    rawRecord.getOptionalBoolean("dd_insurance_yn").fold(dog) { insurance =>
      if (insurance) {
        val provider = rawRecord.getOptionalNumber("dd_insurance")
        dog.copy(
          ddInsurance = Some(insurance),
          ddInsuranceProvider = provider,
          ddInsuranceProviderOtherDescription =
            if (provider.contains(98)) rawRecord.getOptionalStripped("dd_insurance_other") else None
        )
      } else {
        dog.copy(ddInsurance = Some(insurance))
      }
    }

  /**
    * Parse all acquisition-related fields out of a raw RedCap record,
    * injecting them into a partially-modeled dog record.
    */
  def mapAcquiredInfo(rawRecord: RawRecord, dog: HlesDogDemographics): HlesDogDemographics = {
    val usBorn = rawRecord.getOptional("dd_us_born")
    val country = usBorn match {
      case Some("1") => Some("US")
      case Some("0") => rawRecord.getOptional("dd_us_born_no")
      case other     => other
    }
    val source = rawRecord.getOptionalNumber("dd_acquire_source")
    val locationKnown = usBorn.contains("1") && rawRecord.getBoolean("dd_acquired_location_yn")

    dog.copy(
      ddAcquiredYear = rawRecord.getOptionalNumber("dd_acquire_year"),
      ddAcquiredMonth = rawRecord.getOptionalNumber("dd_acquire_month"),
      ddAcquiredSource = source,
      ddAcquiredSourceOtherDescription =
        if (source.contains(98)) rawRecord.getOptionalStripped("dd_acquire_source_other") else None,
      ddAcquiredCountry = country,
      ddAcquiredState = if (locationKnown) rawRecord.getOptional("dd_acquired_st") else None
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
  def mapActivities(rawRecord: RawRecord, dog: HlesDogDemographics): HlesDogDemographics = {
    val allActivities = rawRecord.getArray("dd_activities").map(_.toLong)

    def activityLevel(activity: String): Option[Long] = {
      // gets activity level regardless of activities_m check
      rawRecord.getOptionalNumber(s"dd_${activity}_m") match {
        case Some(value) => Some(value)
        case None =>
          if (allActivities.contains[Long](ActivityValues(activity))) {
            Some(3L)
          } else {
            None
          }
      }
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
        case 1L => rawRecord.getOptionalStripped("dd_1st_activity_other")
        case 2L => rawRecord.getOptionalStripped("dd_2nd_activity_other")
        case _  => None
      },
      ddActivitiesServiceSeeingEye = serviceTypes.map(_.contains("1")),
      ddActivitiesServiceHearingOrSignal = serviceTypes.map(_.contains("2")),
      ddActivitiesServiceWheelchair = serviceTypes.map(_.contains("3")),
      ddActivitiesServiceOtherMedical = otherMedService,
      ddActivitiesServiceOtherMedicalDescription =
        if (otherMedService.contains(true))
          rawRecord.getOptionalStripped("dd_service_medical_other_1")
        else None,
      ddActivitiesServiceOtherHealth = otherHealthService,
      ddActivitiesServiceOtherHealthDescription =
        if (otherHealthService.contains(true))
          rawRecord.getOptionalStripped("dd_service_health_other_1")
        else None,
      ddActivitiesServiceCommunityTherapy = serviceTypes.map(_.contains("6")),
      ddActivitiesServiceEmotionalSupport = serviceTypes.map(_.contains("7")),
      ddActivitiesServiceOther = otherService,
      ddActivitiesServiceOtherDescription =
        if (otherService.contains(true)) rawRecord.getOptionalStripped("dd_service_other_1")
        else None
    )
  }
}
