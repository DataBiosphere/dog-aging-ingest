package org.broadinstitute.monster.dap

import java.time.{LocalDate, Period}

import com.spotify.scio.ScioContext
import com.spotify.scio.coders.Coder
import com.spotify.scio.values.SCollection
import org.broadinstitute.monster.common.{PipelineBuilder, StorageIO}
import org.broadinstitute.monster.common.msg._
import org.broadinstitute.monster.dogaging.jadeschema.struct.HlesDogResidence
import org.broadinstitute.monster.dogaging.jadeschema.table.{HlesDog, HlesOwner}
import upack.Msg

object HLESurveyTransformationPipelineBuilder extends PipelineBuilder[Args] {

  implicit val msgCoder: Coder[Msg] = Coder.beam(new UpackMsgCoder)

  case class RawRecord(id: Long, fields: Map[String, Array[String]]) {

    def getRequired(field: String): String = {
      val values = fields.getOrElse(field, Array.empty)
      if (values.length != 1) {
        throw new IllegalStateException(s"Record $id has more/less than 1 value for field $field")
      } else {
        values.head
      }
    }

    def getOptional(field: String): Option[String] = {
      val values = fields.getOrElse(field, Array.empty)
      if (values.length > 1) {
        throw new IllegalStateException(s"Record $id has multiple values for field $field")
      } else {
        values.headOption
      }
    }

    def getArray(field: String): Array[String] = fields.getOrElse(field, Array.empty)
  }

  /**
    * Schedule all the steps for the Dog Aging transformation in the given pipeline context.
    *
    * Scheduled steps are launched against the context's runner when the `run()` method
    * is called on it.
    */
  override def buildPipeline(ctx: ScioContext, args: Args): Unit = {
    val rawRecords = readRecords(ctx, args)
    val dogs = rawRecords.transform("Map Dogs")(_.map(mapDog))
    val owners = rawRecords.transform("Map Owners")(_.map(mapOwner))

    StorageIO.writeJsonLists(dogs, "Dogs", s"${args.outputPrefix}/hles_dog")
    StorageIO.writeJsonLists(owners, "Owners", s"${args.outputPrefix}/hles_owner")
    ()
  }

  /**
    * Read in records and group by study Id, with field name subgroups.
    * Output the format: (studyId, Iterable[(fieldName, Iterable[value])])
    */
  def readRecords(ctx: ScioContext, args: Args): SCollection[RawRecord] = {
    val rawRecords = StorageIO
      .readJsonLists(
        ctx,
        "Raw Records",
        s"${args.inputPrefix}/records/*.json"
      )

    // Group by study ID (record number) and field name
    // to get the format: (studyId, Iterable((fieldName, Iterable(value))))
    rawRecords
      .groupBy(_.read[String]("record"))
      .map {
        case (id, rawRecordValues) =>
          val fields = rawRecordValues
            .groupBy(_.read[String]("field_name"))
            .map {
              case (fieldName, rawValues) =>
                (fieldName, rawValues.map(_.read[String]("value")).toArray.sorted)
            }
          RawRecord(id.toLong, fields)
      }
  }

  def mapOwner(rawRecord: RawRecord): HlesOwner = HlesOwner(
    // FIXME: Once DAP figures out a name for a dedicated owner ID, use that.
    ownerId = rawRecord.id,
    odAgeRangeYears = None,
    odMaxEducation = None,
    odMaxEducationOther = None,
    odRace = Array.empty,
    odRaceOther = None,
    odHispanic = None,
    odAnnualIncomeRangeUsd = None,
    ocHouseholdAdultCount = None,
    ocHouseholdChildCount = None,
    ssHouseholdDogCount = None,
    ocPrimaryResidenceState = None,
    ocPrimaryResidenceCensusDivision = None,
    ocPrimaryResidenceZip = None,
    ocPrimaryResidenceOwnership = None,
    ocPrimaryResidenceOwnershipOther = None,
    ocSecondaryResidenceState = None,
    ocSecondaryResidenceZip = None,
    ocSecondaryResidenceOwnership = None,
    ocSecondaryResidenceOwnershipOther = None
  )

  def mapDog(rawRecord: RawRecord): HlesDog = {
    val dogBase = HlesDog(
      dogId = rawRecord.id,
      // FIXME: Once DAP figures out a name for a dedicated owner ID, use that.
      ownerId = rawRecord.id,
      // withBreed
      ddBreedPure = None,
      ddBreedPureNonAkc = None,
      ddBreedMixedPrimary = None,
      ddBreedMixedSecondary = None,
      // withAge
      ddAgeYears = None,
      ddAgeBasis = None,
      ddAgeExactSource = Array.empty,
      ddAgeExactSourceOther = None,
      ddAgeEstimateSource = Array.empty,
      ddAgeEstimateSourceOther = None,
      ddBirthYear = None,
      ddBirthMonth = None,
      // withSex
      ddSex = None,
      // withSpayNeuter
      ddSpayedOrNeutered = None,
      ddSpayOrNeuterAge = None,
      ddSpayMethod = None,
      ddEstrousCycleExperiencedBeforeSpayed = None,
      ddEstrousCycleCount = None,
      ddHasBeenPregnant = None,
      ddHasSiredLitters = None,
      ddLitterCount = None,
      // withWeight
      ddWeightRange = None,
      ddWeightLbs = None,
      ddWeightRangeExpectedAdult = None,
      // withInsurance
      ddInsuranceProvider = None,
      ddInsuranceProviderOther = None,
      // withAcquired
      ddAcquiredYear = None,
      ddAcquiredMonth = None,
      ddAcquiredSource = None,
      ddAcquiredSourceOther = None,
      ddAcquiredCountry = None,
      ddAcquiredState = None,
      ddAcquiredZip = None,
      // withRoles
      ddPrimaryRole = None,
      ddPrimaryRoleOther = None,
      ddSecondaryRole = None,
      ddSecondaryRoleOther = None,
      ddOtherRoles = Array.empty,
      ddServiceTypes = Array.empty,
      ddServiceTypesOtherMedical = None,
      ddServiceTypesOtherHealth = None,
      ddServiceTypesOther = None,
      // withResidences
      ocPrimaryResidenceState = None,
      ocPrimaryResidenceCensusDivision = None,
      ocPrimaryResidenceZip = None,
      ocPrimaryResidenceOwnership = None,
      ocPrimaryResidenceOwnershipOther = None,
      ocPrimaryResidenceTimePercentage = None,
      ocSecondaryResidenceState = None,
      ocSecondaryResidenceZip = None,
      ocSecondaryResidenceOwnership = None,
      ocSecondaryResidenceOwnershipOther = None,
      ocSecondaryResidenceTimePercentage = None,
      ddOtherRecentShortTermResidences = Array.empty
    )

    val withBreed = {
      val isPureBreed = rawRecord.getRequired("dd_dog_pure_or_mixed") == "1"
      if (isPureBreed) {
        val breed = rawRecord.getOptional("dd_dog_breed")
        dogBase.copy(
          ddBreedPure = breed,
          ddBreedPureNonAkc =
            if (breed.contains("277")) rawRecord.getOptional("dd_dog_breed_non_akc") else None
        )
      } else {
        dogBase.copy(
          ddBreedMixedPrimary = rawRecord.getOptional("dd_dog_breed_mix_1"),
          ddBreedMixedSecondary = rawRecord.getOptional("dd_dog_breed_mix_2")
        )
      }
    }

    val withAge = {
      val ageCertain = rawRecord.getRequired("dd_dog_birth_year_certain") == "1"

      /*
       * FIXME: How do we encode this in the data?
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
        val exactMonthKnown = rawRecord.getRequired("dd_dog_birth_month_yn") == "1"
        val birthMonth = if (exactMonthKnown) {
          rawRecord.getRequired("dd_dog_birth_month").toInt
        } else {
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

        withBreed.copy(
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

        withBreed.copy(
          ddAgeYears = rawRecord.getOptional("dd_dog_age").map(_.toDouble),
          ddAgeBasis = Some("3"),
          ddAgeEstimateSource = source,
          ddAgeEstimateSourceOther =
            if (source.contains("98")) rawRecord.getOptional("dd_dog_age_estimate_other") else None
        )
      }
    }

    val withSex = withAge.copy(ddSex = rawRecord.getOptional("dd_dog_sex"))

    val withSpayNeuter = {
      val spayedOrNeutered = rawRecord.getRequired("dd_dog_spay_neuter") == "1"
      val spayOrNeuterAge = if (spayedOrNeutered) {
        rawRecord.getOptional("dd_spay_or_neuter_age")
      } else {
        None
      }
      val male = withSex.ddSex.contains("1")

      if (male) {
        val siredLitters = if (spayedOrNeutered) {
          rawRecord.getOptional("dd_ms_sired_yn")
        } else {
          rawRecord.getOptional("dd_mns_sired_yn")
        }
        val litterCountField =
          if (spayedOrNeutered) "dd_mns_nbr_litters_2" else "dd_mns_nbr_litters"
        withSex.copy(
          ddSpayedOrNeutered = Some(spayedOrNeutered),
          ddSpayOrNeuterAge = spayOrNeuterAge,
          ddHasSiredLitters = siredLitters,
          ddLitterCount =
            if (siredLitters.contains("1")) rawRecord.getOptional(litterCountField) else None
        )
      } else {
        val abbrev = if (spayedOrNeutered) "fs" else "fns"
        val pregnant = rawRecord.getOptional(s"dd_${abbrev}_pregnant")
        withSex.copy(
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

    val withWeight = withSpayNeuter.copy(
      ddWeightRange = rawRecord.getOptional("dd_dog_weight"),
      ddWeightLbs = rawRecord.getOptional("dd_dog_weight_lbs").map(_.toDouble),
      ddWeightRangeExpectedAdult = rawRecord.getOptional("dd_weight_range_expected_adult")
    )

    val withInsurance = if (rawRecord.getOptional("dd_insurance_yn").contains("1")) {
      val provider = rawRecord.getOptional("dd_insurance")
      withWeight.copy(
        ddInsuranceProvider = provider,
        ddInsuranceProviderOther =
          if (provider.contains("98")) rawRecord.getOptional("dd_insurance_other") else None
      )
    } else {
      withWeight
    }

    val withAcquired = {
      val usBorn = rawRecord.getRequired("dd_us_born")
      val country = usBorn match {
        case "1"   => Some("US")
        case "0"   => rawRecord.getOptional("dd_us_born_no")
        case other => Some(other)
      }
      val source = rawRecord.getOptional("dd_acquire_source")
      val locationKnown = rawRecord.getOptional("dd_acquired_location_yn").contains("1")

      withInsurance.copy(
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

    // Very unfortunate that we have to hard-code this.
    val roleToLabel = Map(
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

    val withRoles = {
      val allRoles = rawRecord.getArray("dd_activities")
      val (primary, secondary, other) =
        allRoles.foldLeft((Option.empty[String], Option.empty[String], List.empty[String])) {
          case ((primaryAcc, secondaryAcc, otherAcc), role) =>
            val roleLabel = roleToLabel(role)
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

      withAcquired.copy(
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

    val withResidences = {
      val hasSecondaryResidence = rawRecord.getOptional("oc_address2_yn").contains("1")
      val hasTertiaryResidences = rawRecord.getOptional("dd_2nd_residence_yn").contains("1")
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

      withRoles.copy(
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

    withResidences
  }
}
