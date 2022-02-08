package org.broadinstitute.monster.dap.afus

import org.broadinstitute.monster.dap.common.OwnerTransformations.getCensusDivision
import org.broadinstitute.monster.dap.common.RawRecord
import org.broadinstitute.monster.dogaging.jadeschema.fragment.AfusDogDemographics

object DemographicsTransformations {

  def mapDemographics(rawRecord: RawRecord): AfusDogDemographics = {
    val init = AfusDogDemographics.init()

    val transformations = List(
      mapGeneral _,
      mapInsurance _,
      mapActivities _,
      mapResidence _,
      mapReproductive _
    )

    transformations.foldLeft(init)((acc, f) => f(rawRecord, acc))
  }

  def mapGeneral(rawRecord: RawRecord, dog: AfusDogDemographics): AfusDogDemographics = {
    dog.copy(
      afusDdDogWeightLbs = rawRecord.getOptional("fu_dd_dog_weight_lbs").map(_.toDouble)
    )
  }

  def mapInsurance(rawRecord: RawRecord, dog: AfusDogDemographics): AfusDogDemographics = {
    rawRecord.getOptionalBoolean("fu_dd_insurance_yn").fold(dog) { insurance =>
      if (insurance) {
        val provider = rawRecord.getOptionalNumber("fu_dd_insurance")
        dog.copy(
          afusDdInsuranceYn = Some(insurance),
          afusDdInsuranceCompany = provider,
          afusDdInsuranceCompanyOther =
            if (provider.contains(98)) rawRecord.getOptionalStripped("fu_dd_insurance_other")
            else None
        )
      } else {
        dog.copy(afusDdInsuranceYn = Some(insurance))
      }
    }
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
  def mapActivities(rawRecord: RawRecord, dog: AfusDogDemographics): AfusDogDemographics = {
    val allActivities = rawRecord.getArray("fu_dd_activities").map(_.toLong)

    def activityLevel(activity: String): Option[Long] =
      if (allActivities.contains[Long](ActivityValues(activity))) {
        // fu_dd_activities contains one activity, that should be the primary
        if (allActivities.length.equals(1)) Some(1L)
        else { rawRecord.getOptionalNumber(s"fu_dd_${activity}_m").orElse(Some(3L)) }
      } else {
        None
      }

    val serviceLevel = activityLevel("service")
    val assistanceLevel = activityLevel("assistance")
    val otherLevel = activityLevel("other")

    val serviceTypes = if (serviceLevel.exists(_ != 3L) || assistanceLevel.exists(_ != 3L)) {
      Some(rawRecord.getArray("fu_dd_service_type_1"))
    } else {
      None
    }
    val otherMedService = serviceTypes.map(_.contains("4"))
    val otherHealthService = serviceTypes.map(_.contains("5"))
    val otherService = serviceTypes.map(_.contains("98"))

    dog.copy(
      afusDdActivitiesCompanionAnimal = activityLevel("companion"),
      afusDdActivitiesObedience = activityLevel("obedience"),
      afusDdActivitiesShow = activityLevel("show"),
      afusDdActivitiesBreeding = activityLevel("breeding"),
      afusDdActivitiesAgility = activityLevel("agility"),
      afusDdActivitiesHunting = activityLevel("hunting"),
      afusDdActivitiesWorking = activityLevel("working"),
      afusDdActivitiesFieldTrials = activityLevel("field"),
      afusDdActivitiesSearchAndRescue = activityLevel("search_rescue"),
      afusDdActivitiesService = serviceLevel,
      afusDdActivitiesAssistanceOrTherapy = assistanceLevel,
      afusDdActivitiesOther = otherLevel,
      afusDdActivitiesOtherDescription = otherLevel.flatMap {
        case 1L => rawRecord.getOptionalStripped("fu_dd_1st_activity_other")
        case _  => None
      },
      afusDdServiceSeeingEye = serviceTypes.map(_.contains("1")),
      afusDdServiceHearingOrSignal = serviceTypes.map(_.contains("2")),
      afusDdServiceWheelchairService = serviceTypes.map(_.contains("3")),
      afusDdServiceOtherMedicalService = otherMedService,
      afusDdServiceOtherHealthAssistance = otherHealthService,
      afusDdServiceTherapy = serviceTypes.map(_.contains("6")),
      afusDdServiceEmotionalSupport = serviceTypes.map(_.contains("7")),
      afusDdServiceOther = otherService,
      afusDdServiceOtherMedicalDescription =
        if (otherMedService.contains(true))
          rawRecord.getOptionalStripped("fu_dd_service_medical_other_1")
        else None,
      afusDdServiceOtherHealthDescription =
        if (otherHealthService.contains(true))
          rawRecord.getOptionalStripped("fu_dd_service_health_other_1")
        else None,
      afusDdServiceOtherServiceDescription =
        if (otherService.contains(true)) rawRecord.getOptionalStripped("fu_dd_service_other_1")
        else None
    )
  }

  def mapResidence(rawRecord: RawRecord, dog: AfusDogDemographics): AfusDogDemographics = {
    val otherLocation = rawRecord.getOptionalBoolean("fu_dd_2nd_residence_yn")
    val primaryAddressChange = rawRecord.getOptionalBoolean("fu_oc_address_change")
    val primaryAddressOwnership =
      if (primaryAddressChange.contains(true)) rawRecord.getOptionalNumber("fu_oc_address1_own")
      else None
    val secondaryAddressChange = rawRecord.getOptionalBoolean("fu_oc_address2_change")
    val secondaryAddressOwnership =
      if (secondaryAddressChange.contains(true)) rawRecord.getOptionalNumber("fu_oc_address2_own")
      else None

    dog.copy(
      afusDdAlternateRecentResidenceCount =
        if (otherLocation.contains(true)) rawRecord.getOptionalNumber("fu_dd_2nd_residence_nbr")
        else Some(0),
      afusOcPrimaryResidenceState =
        if (primaryAddressChange.contains(true)) rawRecord.getOptional("fu_oc_address1_state")
        else None,
      afusOcPrimaryResidenceCensusDivision =
        if (primaryAddressChange.contains(true))
          rawRecord.getOptional("fu_oc_address1_division").flatMap {
            getCensusDivision(_)
          }
        else None,
      afusOcPrimaryResidenceOwnership = primaryAddressOwnership,
      afusOcPrimaryResidenceOwnershipOtherDescription =
        if (primaryAddressOwnership.contains(98))
          rawRecord.getOptionalStripped("fu_oc_address1_own_other")
        else None,
      afusOcPrimaryResidenceTimePercentage =
        if (primaryAddressChange.contains(true)) rawRecord.getOptionalNumber("fu_oc_address1_pct")
        else None,
      afusOcSecondaryResidence = rawRecord.getOptionalNumber("fu_oc_address2_yn"),
      afusOcSecondaryResidenceState =
        if (secondaryAddressChange.contains(true)) rawRecord.getOptional("fu_oc_address2_state")
        else None,
      afusOcSecondaryResidenceOwnership = secondaryAddressOwnership,
      afusOcSecondaryResidenceOwnershipOtherDescription =
        if (secondaryAddressOwnership.contains(98))
          rawRecord.getOptionalStripped("fu_oc_address2_own_other")
        else None,
      afusOcSecondaryResidenceTimePercentage =
        if (secondaryAddressChange.contains(true))
          rawRecord.getOptionalNumber("fu_oc_2nd_address_pct")
        else None,
      afusDdAlternateRecentResidence1State =
        rawRecord.getOptionalNumber("fu_dd_2nd_residence_01_st"),
      afusDdAlternateRecentResidence1Weeks =
        rawRecord.getOptionalNumber("fu_dd_2nd_residence_01_time"),
      afusDdAlternateRecentResidence2State =
        rawRecord.getOptionalNumber("fu_dd_2nd_residence_02_st"),
      afusDdAlternateRecentResidence2Weeks =
        rawRecord.getOptionalNumber("fu_dd_2nd_residence_02_time"),
      afusDdAlternateRecentResidence3State =
        rawRecord.getOptionalNumber("fu_dd_2nd_residence_03_st"),
      afusDdAlternateRecentResidence3Weeks =
        rawRecord.getOptionalNumber("fu_dd_2nd_residence_03_time"),
      afusDdAlternateRecentResidence4State =
        rawRecord.getOptionalNumber("fu_dd_2nd_residence_04_st"),
      afusDdAlternateRecentResidence4Weeks =
        rawRecord.getOptionalNumber("fu_dd_2nd_residence_04_time"),
      afusDdAlternateRecentResidence5State =
        rawRecord.getOptionalNumber("fu_dd_2nd_residence_05_st"),
      afusDdAlternateRecentResidence5Weeks =
        rawRecord.getOptionalNumber("fu_dd_2nd_residence_05_time"),
      afusDdAlternateRecentResidence6State =
        rawRecord.getOptionalNumber("fu_dd_2nd_residence_06_st"),
      afusDdAlternateRecentResidence6Weeks =
        rawRecord.getOptionalNumber("fu_dd_2nd_residence_06_time"),
      afusDdAlternateRecentResidence7State =
        rawRecord.getOptionalNumber("fu_dd_2nd_residence_07_st"),
      afusDdAlternateRecentResidence7Weeks =
        rawRecord.getOptionalNumber("fu_dd_2nd_residence_07_time"),
      afusDdAlternateRecentResidence8State =
        rawRecord.getOptionalNumber("fu_dd_2nd_residence_08_st"),
      afusDdAlternateRecentResidence8Weeks =
        rawRecord.getOptionalNumber("fu_dd_2nd_residence_08_time"),
      afusDdAlternateRecentResidence9State =
        rawRecord.getOptionalNumber("fu_dd_2nd_residence_09_st"),
      afusDdAlternateRecentResidence9Weeks =
        rawRecord.getOptionalNumber("fu_dd_2nd_residence_09_time"),
      afusDdAlternateRecentResidence10State =
        rawRecord.getOptionalNumber("fu_dd_2nd_residence_10_st"),
      afusDdAlternateRecentResidence10Weeks =
        rawRecord.getOptionalNumber("fu_dd_2nd_residence_10_time"),
      afusDdAlternateRecentResidence11State =
        rawRecord.getOptionalNumber("fu_dd_2nd_residence_11_st"),
      afusDdAlternateRecentResidence11Weeks =
        rawRecord.getOptionalNumber("fu_dd_2nd_residence_11_time"),
      afusDdAlternateRecentResidence12State =
        rawRecord.getOptionalNumber("fu_dd_2nd_residence_12_st"),
      afusDdAlternateRecentResidence12Weeks =
        rawRecord.getOptionalNumber("fu_dd_2nd_residence_12_time"),
      afusDdAlternateRecentResidence13State =
        rawRecord.getOptionalNumber("fu_dd_2nd_residence_13_st"),
      afusDdAlternateRecentResidence13Weeks =
        rawRecord.getOptionalNumber("fu_dd_2nd_residence_13_time")
    )
  }

  def mapReproductive(rawRecord: RawRecord, dog: AfusDogDemographics): AfusDogDemographics = {
    dog.copy(
      afusDdFnsLitterYn = rawRecord.getOptionalBoolean("fu_dd_fns_litter_yn"),
      afusDdFnsHeatYn = rawRecord.getOptionalBoolean("fu_dd_fns_heat_yn"),
      afusDdFnsHormoneYn = rawRecord.getOptionalBoolean("fu_dd_fns_hormone_yn"),
      afusDdFnsHormoneNameYn = rawRecord.getOptionalNumber("fu_dd_fns_hormone_name_yn"),
      afusDdFnsHormoneName = rawRecord.getOptional("fu_dd_fns_hormone_name"),
      afusDdFnsHormoneWeeks = rawRecord.getOptionalNumber("fu_dd_fns_hormone_weeks"),
      afusDdFnsNonpregnantBreedings =
        rawRecord.getOptionalBoolean("fu_dd_fns_nonpregnant_breedings"),
      afusDdFnsPregnancyTermination =
        rawRecord.getOptionalBoolean("fu_dd_fns_pregnancy_termination"),
      afusDdFnsCurrentlyPregnant = rawRecord.getOptionalBoolean("fu_dd_fns_currently_pregnant"),
      afusDdFnsLitterDate = rawRecord.getOptionalDate("fu_dd_fns_litter_date"),
      afusDdFnsSireBreed = rawRecord.getOptionalBoolean("fu_dd_fns_sire_breed"),
      afusDdFnsLitterSize = rawRecord.getOptionalNumber("fu_dd_fns_litter_size"),
      afusDdFnsLitterAllLive = rawRecord.getOptionalBoolean("fu_dd_fns_litter_all_alive_yn"),
      afusDdFnsLitterDeadCount = rawRecord.getOptionalNumber("fu_dd_fns_litter_number_dead"),
      afusDdFnsLitterNurseWeeks = rawRecord.getOptionalNumber("fu_dd_fns_litter_nurse_weeks"),
      afusDdFnsLitterPuppyDeformities =
        rawRecord.getOptionalBoolean("fu_dd_fns_litter_deformities"),
      afusDdFnsLitterDeliveryMedicalIntervention =
        rawRecord.getOptionalBoolean("fu_dd_fns_litter_med_intervention"),
      afusDdFnsLitterDeliverySurgicalIntervention =
        rawRecord.getOptionalBoolean("fu_dd_fns_litter_surg_intervention"),
      afusDdFnsSpay = rawRecord.getOptionalBoolean("fu_dd_dog_spay"),
      afusDdFnsSpayMonth = rawRecord.getOptionalNumber("fu_dd_fs_month"),
      afusDdFnsSpayYear = rawRecord.getOptionalNumber("fu_dd_fs_year"),
      afusDdFnsSpayReason = rawRecord.getOptionalNumber("fu_dd_fs_why_spay"),
      afusDdFnsSpayReasonOtherDescription =
        rawRecord.getOptionalStripped("fu_dd_fs_why_spay_other"),
      afusDdFnsSpayDuringHeat = rawRecord.getOptionalNumber("fu_dd_fs_spay_during_heat"),
      afusDdFnsSpayMethod = rawRecord.getOptionalNumber("fu_dd_fs_spay_method"),
      afusDdMnnSiredYn = rawRecord.getOptionalNumber("fu_dd_mnn_sired_yn"),
      afusDdMnnNbrLitters = rawRecord.getOptionalNumber("fu_dd_mnn_nbr_litters"),
      afusDdMnnDamBreedLitter1 = rawRecord.getOptionalNumber("fu_dd_mnn_dam_breed_litter_1"),
      afusDdMnnDamBreedLitter2 = rawRecord.getOptionalNumber("fu_dd_mnn_dam_breed_litter_2"),
      afusDdMnnDamBreedLitter3 = rawRecord.getOptionalNumber("fu_dd_mnn_dam_breed_litter_3"),
      afusDdMnnDamBreedLitter4 = rawRecord.getOptionalNumber("fu_dd_mnn_dam_breed_litter_4"),
      afusDdMnnDamBreedLitter5 = rawRecord.getOptionalNumber("fu_dd_mnn_dam_breed_litter_5"),
      afusDdMnnDamBreedLitter6 = rawRecord.getOptionalNumber("fu_dd_mnn_dam_breed_litter_6"),
      afusDdMnnDamBreedLitter7 = rawRecord.getOptionalNumber("fu_dd_mnn_dam_breed_litter_7"),
      afusDdMnnDamBreedLitter8 = rawRecord.getOptionalNumber("fu_dd_mnn_dam_breed_litter_8"),
      afusDdMnnDamBreedLitter9 = rawRecord.getOptionalNumber("fu_dd_mnn_dam_breed_litter_9"),
      afusDdMnnDamBreedLitter10 = rawRecord.getOptionalNumber("fu_dd_mnn_dam_breed_litter_10"),
      afusDdMnnNeutered = rawRecord.getOptionalBoolean("fu_dd_dog_neuter"),
      afusDdMnnNeuterMonth = rawRecord.getOptionalNumber("fu_dd_mn_month"),
      afusDdMnnNeuterYear = rawRecord.getOptionalNumber("fu_dd_mn_year"),
      afusDdMnnNeuterReason = rawRecord.getOptionalNumber("fu_dd_mn_why_neuter"),
      afusDdMnnNeuterReasonOtherDescription =
        rawRecord.getOptionalStripped("fu_dd_mn_why_neuter_other"),
      afusDdSpayedOrNeutered = rawRecord.getOptionalNumber("fu_dd_dog_spay_neuter")
    )
  }

}
