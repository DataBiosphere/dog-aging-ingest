package org.broadinstitute.monster.dap.dog

import org.broadinstitute.monster.dap.RawRecord
import org.broadinstitute.monster.dogaging.jadeschema.fragment.HlesDogMedsPreventatives

object MedsAndPreventativesTransformations {

  /** Map all fields from the meds & preventatives RedCap instrument into our target schema. */
  def mapMedsPreventatives(rawRecord: RawRecord): HlesDogMedsPreventatives = {
    val transforms = List(
      mapDental _,
      mapProGroom _,
      mapHomeGroom _,
      mapFleaTick _,
      mapHeartworm _,
      mapOtherMeds _
    )

    transforms.foldLeft(HlesDogMedsPreventatives.init())((acc, f) => f(rawRecord, acc))
  }

  /** Map fields related to dental work. */
  def mapDental(rawRecord: RawRecord, dog: HlesDogMedsPreventatives): HlesDogMedsPreventatives = {
    val dentalCleaning = rawRecord.getOptionalBoolean("mp_de_cleaning")
    val dentalExtraction = rawRecord.getOptionalBoolean("mp_de_extraction")

    dog.copy(
      mpDentalExaminationFrequency = rawRecord.getOptionalNumber("mp_de_examine"),
      mpDentalBrushingFrequency = rawRecord.getOptionalNumber("mp_de_brush"),
      mpDentalTreatFrequency = rawRecord.getOptionalNumber("mp_de_treat"),
      mpDentalFoodFrequency = rawRecord.getOptionalNumber("mp_de_food"),
      mpDentalBreathFreshenerFrequency = rawRecord.getOptionalNumber("mp_de_freshen"),
      mpDentalProcedureUndergone = rawRecord.getOptionalBoolean("mp_de_procedure"),
      mpDentalCleaning = dentalCleaning,
      mpDentalCleaningMonthsAgo = dentalCleaning.flatMap {
        if (_) rawRecord.getOptionalNumber("mp_de_clean_when") else None
      },
      mpDentalExtraction = dentalExtraction,
      mpDentalExtractionMonthsAgo = dentalExtraction.flatMap {
        if (_) rawRecord.getOptionalNumber("mp_de_extraction_when") else None
      }
    )
  }

  /** Map fields related to professional grooming. */
  def mapProGroom(rawRecord: RawRecord, dog: HlesDogMedsPreventatives): HlesDogMedsPreventatives =
    rawRecord.getOptionalBoolean("mp_gr_pro").fold(dog) { proGrooming =>
      if (proGrooming) {
        val shampoos = rawRecord.get("mp_gr_pro_shampoo")
        dog.copy(
          mpProfessionalGrooming = Some(proGrooming),
          mpProfessionalGroomingFrequency = rawRecord.getOptionalNumber("mp_gr_pro_freq"),
          mpProfessionalGroomingShampoosRegular = shampoos.map(_.contains("1")),
          mpProfessionalGroomingShampoosFleaOrTickControl = shampoos.map(_.contains("2")),
          mpProfessionalGroomingShampoosMedicated = shampoos.map(_.contains("3")),
          mpProfessionalGroomingShampoosNone = shampoos.map(_.contains("4")),
          mpProfessionalGroomingShampoosUnknown = shampoos.map(_.contains("99")),
          mpProfessionalGroomingShampoosOther = shampoos.map(_.contains("98")),
          mpProfessionalGroomingShampoosOtherDescription = if (shampoos.exists(_.contains("98"))) {
            rawRecord.getOptionalStripped("mp_gr_pro_shampoo_other")
          } else {
            None
          }
        )
      } else {
        dog.copy(mpProfessionalGrooming = Some(proGrooming))
      }
    }

  /** Map fields related to home grooming. */
  def mapHomeGroom(rawRecord: RawRecord, dog: HlesDogMedsPreventatives): HlesDogMedsPreventatives =
    rawRecord.getOptionalBoolean("mp_gr_home").fold(dog) { homeGrooming =>
      if (homeGrooming) {
        val shampoos = rawRecord.get("mp_gr_home_shampoo")
        dog.copy(
          mpHomeGrooming = Some(homeGrooming),
          mpHomeGroomingFrequency = rawRecord.getOptionalNumber("mp_gr_home_freq"),
          mpHomeGroomingShampoosRegular = shampoos.map(_.contains("1")),
          mpHomeGroomingShampoosFleaOrTickControl = shampoos.map(_.contains("2")),
          mpHomeGroomingShampoosMedicated = shampoos.map(_.contains("3")),
          mpHomeGroomingShampoosNone = shampoos.map(_.contains("4")),
          mpHomeGroomingShampoosUnknown = shampoos.map(_.contains("99")),
          mpHomeGroomingShampoosOther = shampoos.map(_.contains("98")),
          mpHomeGroomingShampoosOtherDescription = if (shampoos.exists(_.contains("98"))) {
            rawRecord.getOptionalStripped("mp_gr_home_shampoo_other")
          } else {
            None
          }
        )
      } else {
        dog.copy(mpHomeGrooming = Some(homeGrooming))
      }
    }

  /** Map fields related to flea and tick prevention. */
  def mapFleaTick(rawRecord: RawRecord, dog: HlesDogMedsPreventatives): HlesDogMedsPreventatives =
    rawRecord.getOptionalBoolean("mp_flea").fold(dog) { fleaTreatment =>
      if (fleaTreatment) {
        val otherTreatment = rawRecord.getOptionalBoolean("mp_flea_other_yn")
        dog.copy(
          mpFleaAndTickTreatment = Some(fleaTreatment),
          mpFleaAndTickTreatmentFrequency = rawRecord.getOptionalNumber("mp_flea_freq"),
          mpFleaAndTickTreatmentTopical = rawRecord.getOptionalBoolean("mp_flea_topical"),
          mpFleaAndTickTreatmentOral = rawRecord.getOptionalBoolean("mp_flea_oral"),
          mpFleaAndTickTreatmentDip = rawRecord.getOptionalBoolean("mp_flea_dip"),
          mpFleaAndTickTreatmentCollar = rawRecord.getOptionalBoolean("mp_flea_collar"),
          mpFleaAndTickTreatmentOther = otherTreatment,
          mpFleaAndTickTreatmentOtherDescription = otherTreatment.flatMap {
            if (_) rawRecord.getOptionalStripped("mp_flea_other") else None
          }
        )
      } else {
        dog.copy(mpFleaAndTickTreatment = Some(fleaTreatment))
      }
    }

  /** Map fields related to heartworm prevention. */
  def mapHeartworm(rawRecord: RawRecord, dog: HlesDogMedsPreventatives): HlesDogMedsPreventatives =
    rawRecord.getOptionalBoolean("mp_hw").fold(dog) { heartwormTreatment =>
      if (heartwormTreatment) {
        val otherTreatment = rawRecord.getOptionalBoolean("mp_hw_other_yn")
        dog.copy(
          mpHeartwormPreventative = Some(heartwormTreatment),
          mpHeartwormPreventativeFrequency = rawRecord.getOptionalNumber("mp_hw_freq"),
          mpHeartwormPreventativeOralChewable = rawRecord.getOptionalBoolean("mp_hw_tablet"),
          mpHeartwormPreventativeOralSolution = rawRecord.getOptionalBoolean("mp_hw_solution"),
          mpHeartwormPreventativeTopical = rawRecord.getOptionalBoolean("mp_hw_topical"),
          mpHeartwormPreventativeInjectable = rawRecord.getOptionalBoolean("mp_hw_injectable"),
          mpHeartwormPreventativeOther = otherTreatment,
          mpHeartwormPreventativeOtherDescription = otherTreatment.flatMap {
            if (_) rawRecord.getOptionalStripped("mp_hw_other") else None
          }
        )
      } else {
        dog.copy(mpHeartwormPreventative = Some(heartwormTreatment))
      }
    }

  /** Map fields related to other medications and preventatives. */
  def mapOtherMeds(
    rawRecord: RawRecord,
    dog: HlesDogMedsPreventatives
  ): HlesDogMedsPreventatives = {
    val otherNonPrescription = rawRecord.getOptionalBoolean("mp_np_other_yn")
    dog.copy(
      mpVaccinationStatus = rawRecord.getOptionalNumber("mp_vacc"),
      mpRecentNonPrescriptionMedsAntibioticOintment =
        rawRecord.getOptionalBoolean("mp_np_ab_ointment"),
      mpRecentNonPrescriptionMedsAntihistamine =
        rawRecord.getOptionalBoolean("mp_np_antihistamine"),
      mpRecentNonPrescriptionMedsAntiInflammatory =
        // This is correct, misspelled in REDCap
        rawRecord.getOptionalBoolean("mp_np_anti_inflamatory"),
      mpRecentNonPrescriptionMedsEarCleaner = rawRecord.getOptionalBoolean("mp_np_ear_cleaner"),
      mpRecentNonPrescriptionMedsEnzymes = rawRecord.getOptionalBoolean("mp_np_enzyme"),
      mpRecentNonPrescriptionMedsEyeLubricant = rawRecord.getOptionalBoolean("mp_np_eye_lube"),
      mpRecentNonPrescriptionMedsJointSupplement = rawRecord.getOptionalBoolean("mp_np_joint"),
      mpRecentNonPrescriptionMedsUpsetStomach = rawRecord.getOptionalBoolean("mp_np_gi_upset"),
      mpRecentNonPrescriptionMedsOmega3 = rawRecord.getOptionalBoolean("mp_np_omega_3"),
      mpRecentNonPrescriptionMedsNonOilSkin = rawRecord.getOptionalBoolean("mp_np_skin"),
      mpRecentNonPrescriptionMedsProbiotic = rawRecord.getOptionalBoolean("mp_np_probiotic"),
      mpRecentNonPrescriptionMedsAcidifySupplement =
        rawRecord.getOptionalBoolean("mp_np_acidify_urine"),
      mpRecentNonPrescriptionMedsAlkalinizeSupplement =
        rawRecord.getOptionalBoolean("mp_np_alkalinize_urine"),
      mpRecentNonPrescriptionMedsVitamin = rawRecord.getOptionalBoolean("mp_np_vitamin"),
      mpRecentNonPrescriptionMedsOther = otherNonPrescription,
      mpRecentNonPrescriptionMedsOtherDescription = otherNonPrescription.flatMap {
        if (_) rawRecord.getOptionalStripped("mp_np_other") else None
      }
    )
  }
}
