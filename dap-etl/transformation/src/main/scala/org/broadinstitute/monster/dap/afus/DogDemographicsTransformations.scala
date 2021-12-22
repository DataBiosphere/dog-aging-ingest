package org.broadinstitute.monster.dap.afus

import org.broadinstitute.monster.dap.common.RawRecord
import org.broadinstitute.monster.dogaging.jadeschema.fragment.AfusDogDemographics

object DogDemographicsTransformations {

  def mapDemographics(rawRecord: RawRecord): AfusDogDemographics = {
    val init = AfusDogDemographics.init()

    val transformations = List(
      mapGeneral _,
      mapInsurance _,
      mapActivities _
    )

    transformations.foldLeft(init)((acc, f) => f(rawRecord, acc))
  }

  def mapGeneral(rawRecord: RawRecord, dog: AfusDogDemographics): AfusDogDemographics = {
    val otherLocation = rawRecord.getOptionalBoolean("fu_dd_2nd_residence_yn")
    dog.copy(
      afusDdDogWeightLbs = rawRecord.getOptional("fu_dd_dog_weight_lbs").map(_.toDouble),
      afusDdAlternateRecentResidenceCount =
        if (otherLocation.contains(true)) rawRecord.getOptionalNumber("fu_dd_2nd_residence_nbr")
        else None
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
}
