package org.broadinstitute.monster.dap.afus

import org.broadinstitute.monster.dap.common.RawRecord
import org.broadinstitute.monster.dogaging.jadeschema.fragment.AfusDogMedsPreventatives

object MedsPreventativesTransformations {

  def mapMedsPreventatives(rawRecord: RawRecord): AfusDogMedsPreventatives = {
    val transformations = List(
      mapDental _,
      mapGroom _,
      mapFleaTick _,
      mapHeartworm _,
      mapOtherMeds _
    )

    transformations.foldLeft(AfusDogMedsPreventatives.init)((acc, f) => f(rawRecord, acc))
  }

  def mapDental(rawRecord: RawRecord, dog: AfusDogMedsPreventatives): AfusDogMedsPreventatives = {
    val dentalCleaning = rawRecord.getOptionalBoolean("fu_mp_de_cleaning")
    val dentalExtraction = rawRecord.getOptionalBoolean("fu_mp_de_extraction")
    dog.copy(
      afusMpDentalExaminationFrequency = rawRecord.getOptionalNumber("fu_mp_de_examine"),
      afusMpDentalBrushingFrequency = rawRecord.getOptionalNumber("fu_mp_de_brush"),
      afusMpDentalTreatFrequency = rawRecord.getOptionalNumber("fu_mp_de_treat"),
      afusMpDentalFoodFrequency = rawRecord.getOptionalNumber("fu_mp_de_food"),
      afusMpDentalBreathFreshenerFrequency = rawRecord.getOptionalNumber("fu_mp_de_freshen"),
      afusMpDentalProcedureUndergone = rawRecord.getOptionalBoolean("fu_mp_de_procedure"),
      afusMpDentalCleaning = dentalCleaning,
      afusMpDentalExtraction = dentalExtraction
    )
  }

  def mapGroom(rawRecord: RawRecord, dog: AfusDogMedsPreventatives): AfusDogMedsPreventatives = {
    dog.copy(
      // mapProGroom
      afusMpProfessionalGrooming = rawRecord.getOptionalBoolean("fu_mp_gr_pro"),
      afusMpProfessionalGroomingFrequency = rawRecord.getOptionalNumber("fu_mp_gr_pro_freq"),
      // mapHomeGroom
      afusMpHomeGrooming = rawRecord.getOptionalBoolean("fu_mp_gr_home"),
      afusMpHomeGroomingFrequency = rawRecord.getOptionalNumber("fu_mp_gr_home_freq")
    )
  }

  def mapFleaTick(rawRecord: RawRecord, dog: AfusDogMedsPreventatives): AfusDogMedsPreventatives = {
    dog.copy(
      // mapFleaTick
      afusMpFleaAndTickTreatment = rawRecord.getOptionalBoolean("fu_mp_flea"),
      afusMpFleaAndTickTreatmentFrequency = rawRecord.getOptionalNumber("fu_mp_flea_freq"),
      afusMpFleaAndTickTreatmentTopical = rawRecord.getOptionalBoolean("fu_mp_flea_topical"),
      afusMpFleaAndTickTreatmentCollar = rawRecord.getOptionalBoolean("fu_mp_flea_collar"),
      afusMpFleaAndTickTreatmentOral = rawRecord.getOptionalBoolean("fu_mp_flea_oral"),
      afusMpFleaAndTickTreatmentDip = rawRecord.getOptionalBoolean("fu_mp_flea_dip"),
      afusMpFleaAndTickTreatmentInjectable = rawRecord.getOptionalBoolean("fu_mp_flea_injectable"),
      afusMpFleaAndTickTreatmentEssentialOils =
        rawRecord.getOptionalBoolean("fu_mp_flea_essential_oils"),
      afusMpFleaAndTickTreatmentShampoo = rawRecord.getOptionalBoolean("fu_mp_flea_shampoo"),
      afusMpFleaAndTickTreatmentEnvironmental =
        rawRecord.getOptionalBoolean("fu_mp_flea_environmental"),
      afusMpFleaAndTickTreatmentOther = rawRecord.getOptionalBoolean("fu_mp_flea_other_yn"),
      afusMpFleaAndTickTreatmentOtherDescription =
        rawRecord.getOptionalStripped("fu_mp_flea_other"),
      afusMpFleaTopicalCollarBrand = rawRecord.getOptionalNumber("fu_mp_flea_topical_collar_brand"),
      afusMpFleaOralBrand = rawRecord.getOptionalNumber("fu_mp_flea_oral_brand")
    )
  }

  def mapHeartworm(
    rawRecord: RawRecord,
    dog: AfusDogMedsPreventatives
  ): AfusDogMedsPreventatives = {
    dog.copy(
      // mapHeartworm
      afusMpHeartwormPreventative = rawRecord.getOptionalBoolean("fu_mp_hw"),
      afusMpHeartwormPreventativeFrequency = rawRecord.getOptionalNumber("fu_mp_hw_freq"),
      afusMpHeartwormPreventativeOralChewable = rawRecord.getOptionalBoolean("fu_mp_hw_tablet"),
      afusMpHeartwormPreventativeOralSolution = rawRecord.getOptionalBoolean("fu_mp_hw_solution"),
      afusMpHeartwormPreventativeTopical = rawRecord.getOptionalBoolean("fu_mp_hw_topical"),
      afusMpHeartwormPreventativeInjectable = rawRecord.getOptionalBoolean("fu_mp_hw_injectable"),
      afusMpHeartwormPreventativeOther = rawRecord.getOptionalBoolean("fu_mp_hw_other_yn"),
      afusMpHeartwormPreventativeOtherDescription = rawRecord.getOptionalStripped("fu_mp_hw_other"),
      afusMpHeartwormPreventativeBrand = rawRecord.getOptionalNumber("fu_mp_hw_brand")
    )
  }

  def mapOtherMeds(
    rawRecord: RawRecord,
    dog: AfusDogMedsPreventatives
  ): AfusDogMedsPreventatives = {
    dog.copy(
      afusMpVaccineRvv = rawRecord.getOptionalNumber("fu_mp_vacc_rvv"),
      afusMpVaccineDcv = rawRecord.getOptionalNumber("fu_mp_vacc_dcv"),
      afusMpVaccineLv = rawRecord.getOptionalNumber("fu_mp_vacc_lv"),
      afusMpVaccineKcv = rawRecord.getOptionalNumber("fu_mp_vacc_kcv"),
      afusMpVaccineCiv = rawRecord.getOptionalNumber("fu_mp_vacc_civ"),
      afusMpVaccineLdv = rawRecord.getOptionalNumber("fu_mp_vacc_ldv"),
      afusMpRecentNonPrescriptionMedsAntibioticOintment =
        rawRecord.getOptionalBoolean("fu_mp_np_ab_ointment"),
      afusMpRecentNonPrescriptionMedsAntihistamine =
        rawRecord.getOptionalBoolean("fu_mp_np_antihistamine"),
      afusMpRecentNonPrescriptionMedsAntiInflammatory =
        rawRecord.getOptionalBoolean("fu_mp_np_anti_inflamatory"),
      afusMpRecentNonPrescriptionMedsEarCleaner =
        rawRecord.getOptionalBoolean("fu_mp_np_ear_cleaner"),
      afusMpRecentNonPrescriptionMedsEnzymes = rawRecord.getOptionalBoolean("fu_mp_np_enzyme"),
      afusMpRecentNonPrescriptionMedsEyeLubricant =
        rawRecord.getOptionalBoolean("fu_mp_np_eye_lube"),
      afusMpRecentNonPrescriptionMedsJointSupplement =
        rawRecord.getOptionalBoolean("fu_mp_np_joint"),
      afusMpRecentNonPrescriptionMedsUpsetStomach =
        rawRecord.getOptionalBoolean("fu_mp_np_gi_upset"),
      afusMpRecentNonPrescriptionMedsOmega3 = rawRecord.getOptionalBoolean("fu_mp_np_omega_3"),
      afusMpRecentNonPrescriptionMedsNonOilSkin = rawRecord.getOptionalBoolean("fu_mp_np_skin"),
      afusMpRecentNonPrescriptionMedsProbiotic = rawRecord.getOptionalBoolean("fu_mp_np_probiotic"),
      afusMpRecentNonPrescriptionMedsAcidifySupplement =
        rawRecord.getOptionalBoolean("fu_mp_np_acidify_urine"),
      afusMpRecentNonPrescriptionMedsAlkalinizeSupplement =
        rawRecord.getOptionalBoolean("fu_mp_np_alkalinize_urine"),
      afusMpRecentNonPrescriptionMedsVitamin = rawRecord.getOptionalBoolean("fu_mp_np_vitamin"),
      afusMpRecentNonPrescriptionMedsOther = rawRecord.getOptionalBoolean("fu_mp_np_other_yn"),
      afusMpRecentNonPrescriptionMedsOtherDescription =
        rawRecord.getOptionalStripped("fu_mp_np_other")
    )
  }
}
