package org.broadinstitute.monster.dap.dog

import org.broadinstitute.monster.dap.common.RawRecord
import org.broadinstitute.monster.dogaging.jadeschema.fragment.HlesDogMedsPreventatives
import org.scalatest.OptionValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class MedsAndPreventativesTransformationsSpec extends AnyFlatSpec with Matchers with OptionValues {
  behavior of "MedsAndPreventativesTransformations"

  it should "map dental fields" in {
    val example = Map(
      "mp_de_cleaning" -> Array("1"),
      "mp_de_extraction" -> Array("0"),
      "mp_de_examine" -> Array("3"),
      "mp_de_brush" -> Array("1"),
      "mp_de_treat" -> Array("4"),
      "mp_de_food" -> Array("0"),
      "mp_de_freshen" -> Array("2"),
      "mp_de_clean_when" -> Array("4")
    )

    val out = MedsAndPreventativesTransformations.mapDental(
      RawRecord(1L, example),
      HlesDogMedsPreventatives.init()
    )

    out.mpDentalExaminationFrequency.value shouldBe 3L
    out.mpDentalBrushingFrequency.value shouldBe 1L
    out.mpDentalTreatFrequency.value shouldBe 4L
    out.mpDentalFoodFrequency.value shouldBe 0L
    out.mpDentalCleaning.value shouldBe true
    out.mpDentalCleaningMonthsAgo.value shouldBe 4L
    out.mpDentalExtraction.value shouldBe false
    out.mpDentalExtractionMonthsAgo shouldBe None
  }

  it should "map professional grooming fields" in {
    val example = Map(
      "mp_gr_pro" -> Array("1"),
      "mp_gr_pro_shampoo" -> Array("1", "3", "98"),
      "mp_gr_pro_shampoo_other" -> Array("Magic shampoo")
    )

    val out = MedsAndPreventativesTransformations.mapProGroom(
      RawRecord(1L, example),
      HlesDogMedsPreventatives.init()
    )

    out.mpProfessionalGrooming.value shouldBe true
    out.mpProfessionalGroomingShampoosRegular.value shouldBe true
    out.mpProfessionalGroomingShampoosFleaOrTickControl.value shouldBe false
    out.mpProfessionalGroomingShampoosMedicated.value shouldBe true
    out.mpProfessionalGroomingShampoosNone.value shouldBe false
    out.mpProfessionalGroomingShampoosUnknown.value shouldBe false
    out.mpProfessionalGroomingShampoosOther.value shouldBe true
    out.mpProfessionalGroomingShampoosOtherDescription.value shouldBe "Magic shampoo"
  }

  it should "map home grooming fields" in {
    val example = Map(
      "mp_gr_home" -> Array("1"),
      "mp_gr_home_shampoo" -> Array("2", "4", "99", "98"),
      "mp_gr_home_shampoo_other" -> Array("Secret sauce")
    )

    val out = MedsAndPreventativesTransformations.mapHomeGroom(
      RawRecord(1L, example),
      HlesDogMedsPreventatives.init()
    )

    out.mpHomeGrooming.value shouldBe true
    out.mpHomeGroomingShampoosRegular.value shouldBe false
    out.mpHomeGroomingShampoosFleaOrTickControl.value shouldBe true
    out.mpHomeGroomingShampoosMedicated.value shouldBe false
    out.mpHomeGroomingShampoosNone.value shouldBe true
    out.mpHomeGroomingShampoosUnknown.value shouldBe true
    out.mpHomeGroomingShampoosOther.value shouldBe true
    out.mpHomeGroomingShampoosOtherDescription.value shouldBe "Secret sauce"
  }

  it should "map flea and tick treatment fields" in {
    val example = Map(
      "mp_flea" -> Array("1"),
      "mp_flea_freq" -> Array("3"),
      "mp_flea_topical" -> Array("1"),
      "mp_flea_oral" -> Array("0"),
      "mp_flea_dip" -> Array("1"),
      "mp_flea_collar" -> Array("0"),
      "mp_flea_other_yn" -> Array("1"),
      "mp_flea_other" -> Array("BIRDS")
    )

    val out = MedsAndPreventativesTransformations.mapFleaTick(
      RawRecord(1L, example),
      HlesDogMedsPreventatives.init()
    )

    out.mpFleaAndTickTreatment.value shouldBe true
    out.mpFleaAndTickTreatmentFrequency.value shouldBe 3L
    out.mpFleaAndTickTreatmentTopical.value shouldBe true
    out.mpFleaAndTickTreatmentOral.value shouldBe false
    out.mpFleaAndTickTreatmentDip.value shouldBe true
    out.mpFleaAndTickTreatmentCollar.value shouldBe false
    out.mpFleaAndTickTreatmentOther.value shouldBe true
    out.mpFleaAndTickTreatmentOtherDescription.value shouldBe "BIRDS"
  }

  it should "map heartworm treatment fields" in {
    val example = Map(
      "mp_hw" -> Array("1"),
      "mp_hw_freq" -> Array("2"),
      "mp_hw_tablet" -> Array("0"),
      "mp_hw_solution" -> Array("1"),
      "mp_hw_topical" -> Array("0"),
      "mp_hw_injectable" -> Array("1"),
      "mp_hw_other_yn" -> Array("0")
    )

    val out = MedsAndPreventativesTransformations.mapHeartworm(
      RawRecord(1L, example),
      HlesDogMedsPreventatives.init()
    )

    out.mpHeartwormPreventative.value shouldBe true
    out.mpHeartwormPreventativeFrequency.value shouldBe 2L
    out.mpHeartwormPreventativeOralChewable.value shouldBe false
    out.mpHeartwormPreventativeOralSolution.value shouldBe true
    out.mpHeartwormPreventativeTopical.value shouldBe false
    out.mpHeartwormPreventativeInjectable.value shouldBe true
    out.mpHeartwormPreventativeOther.value shouldBe false
    out.mpHeartwormPreventativeOtherDescription shouldBe None
  }

  it should "map vaccination and non-prescription medication fields" in {
    val example = Map(
      "mp_vacc" -> Array("2"),
      "mp_np_ab_ointment" -> Array("1"),
      "mp_np_antihistamine" -> Array("0"),
      "mp_np_anti_inflamatory" -> Array("1"),
      "mp_np_ear_cleaner" -> Array("0"),
      "mp_np_enzyme" -> Array("1"),
      "mp_np_eye_lube" -> Array("0"),
      "mp_np_joint" -> Array("1"),
      "mp_np_gi_upset" -> Array("0"),
      "mp_np_omega_3" -> Array("1"),
      "mp_np_skin" -> Array("0"),
      "mp_np_probiotic" -> Array("1"),
      "mp_np_acidify_urine" -> Array("0"),
      "mp_np_alkalinize_urine" -> Array("1"),
      "mp_np_vitamin" -> Array("0"),
      "mp_np_other_yn" -> Array("1"),
      "mp_np_other" -> Array("Voodoo")
    )

    val out = MedsAndPreventativesTransformations.mapOtherMeds(
      RawRecord(1L, example),
      HlesDogMedsPreventatives.init()
    )

    out.mpVaccinationStatus.value shouldBe 2L
    out.mpRecentNonPrescriptionMedsAntibioticOintment.value shouldBe true
    out.mpRecentNonPrescriptionMedsAntihistamine.value shouldBe false
    out.mpRecentNonPrescriptionMedsAntiInflammatory.value shouldBe true
    out.mpRecentNonPrescriptionMedsEarCleaner.value shouldBe false
    out.mpRecentNonPrescriptionMedsEnzymes.value shouldBe true
    out.mpRecentNonPrescriptionMedsEyeLubricant.value shouldBe false
    out.mpRecentNonPrescriptionMedsJointSupplement.value shouldBe true
    out.mpRecentNonPrescriptionMedsUpsetStomach.value shouldBe false
    out.mpRecentNonPrescriptionMedsOmega3.value shouldBe true
    out.mpRecentNonPrescriptionMedsNonOilSkin.value shouldBe false
    out.mpRecentNonPrescriptionMedsProbiotic.value shouldBe true
    out.mpRecentNonPrescriptionMedsAcidifySupplement.value shouldBe false
    out.mpRecentNonPrescriptionMedsAlkalinizeSupplement.value shouldBe true
    out.mpRecentNonPrescriptionMedsVitamin.value shouldBe false
    out.mpRecentNonPrescriptionMedsOther.value shouldBe true
    out.mpRecentNonPrescriptionMedsOtherDescription.value shouldBe "Voodoo"
  }
}
