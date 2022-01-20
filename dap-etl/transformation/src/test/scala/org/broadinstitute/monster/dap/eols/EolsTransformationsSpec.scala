package org.broadinstitute.monster.dap.eols

import org.broadinstitute.monster.dap.common.RawRecord
import org.broadinstitute.monster.dogaging.jadeschema.table.Eols

import java.time.LocalDate
import org.scalatest.OptionValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class EolsTransformationsSpec extends AnyFlatSpec with Matchers with OptionValues {
  behavior of "EolsTransformations"

  it should "map required general eols fields if willing" in {
    val mapped: Eols = EolsTransformations
      .mapEols(
        RawRecord(
          1,
          Map(
            "eol_willing_to_complete" -> Array("1"),
            "eol_know_date_dog_died_yn" -> Array("1"),
            "eol_date_dog_died" -> Array("1990-01-30"),
            "eol_cause1_death" -> Array("1"),
            "eol_cause1_death_specify" -> Array("Doggo lived to a ripe old age of 18"),
            "eol_cause2_death" -> Array("7"),
            "eol_cause2_death_specify" -> Array("He passed away\n\nin his sleep"),
            "eol_old_age_a" -> Array("19"),
            "eol_old_age_a_specify" -> Array("He wagged his tail till his last day"),
            "eol_trauma_a" -> Array("98"),
            "eol_trauma_a_specify" -> Array("He seemed to pass\t in and out of consciousness"),
            "eol_toxin_a" -> Array("98"),
            "eol_toxin_a_specify" -> Array("Other"),
            "eol_old_age_cod_2" -> Array("5"),
            "eol_old_age_cod_2_specify" -> Array("General soreness and stiffness in the body\r\n"),
            "eol_notes" -> Array("Notes on end of life"),
            "eol_add_med_record_yn" -> Array("1")
          )
        )
      )
      .get

    mapped.eolWillingToComplete shouldBe Some(true)
    mapped.eolExactDeathDateKnown shouldBe Some(true)
    mapped.eolDeathDate.value shouldBe LocalDate.of(1990, 1, 30)
    mapped.eolCauseOfDeathPrimary shouldBe Some(1L)
    mapped.eolCauseOfDeathPrimaryOtherDescription shouldBe Some(
      "Doggo lived to a ripe old age of 18"
    )
    mapped.eolCauseOfDeathSecondary shouldBe Some(7L)
    mapped.eolCauseOfDeathSecondaryOtherDescription shouldBe Some("He passed away  in his sleep")
    mapped.eolOldAgePrimary shouldBe Some(19L)
    mapped.eolOldAgePrimaryOtherDescription shouldBe Some("He wagged his tail till his last day")
    mapped.eolTrauma shouldBe Some(98)
    mapped.eolTraumaOtherDescription shouldBe Some("He seemed to pass  in and out of consciousness")
    mapped.eolToxin shouldBe Some(98)
    mapped.eolToxinOtherDescription shouldBe Some("Other")
    mapped.eolOldAgeSecondary shouldBe Some(5L)
    mapped.eolOldAgeSecondaryOtherDescription shouldBe Some(
      "General soreness and stiffness in the body"
    )
    mapped.eolNotesDescription shouldBe Some("Notes on end of life")
    mapped.eolAddVemr shouldBe Some(true)
  }

  it should "not map any eols fields if not willing" in {
    val mapped: Option[Eols] = EolsTransformations
      .mapEols(
        RawRecord(
          1,
          Map(
            "eol_willing_to_complete" -> Array("0"),
            "eol_know_date_dog_died_yn" -> Array("1"),
            "eol_date_dog_died" -> Array("1990-01-01"),
            "eol_cause1_death" -> Array("1"),
            "eol_cause1_death_specify" -> Array("Doggo lived to a ripe old age of 18"),
            "eol_cause2_death" -> Array("7"),
            "eol_cause2_death_specify" -> Array("He passed away in his sleep"),
            "eol_old_age_a" -> Array("19"),
            "eol_old_age_a_specify" -> Array("He wagged his tail till his last day"),
            "eol_trauma_a" -> Array("98"),
            "eol_trauma_a_specify" -> Array("He seemed to in and out of consciousness"),
            "eol_toxin_a" -> Array("98"),
            "eol_toxin_a_specify" -> Array("Other"),
            "eol_old_age_cod_2" -> Array("5"),
            "eol_old_age_cod_2_specify" -> Array("General soreness and stiffness in the body"),
            "eol_notes" -> Array("Notes on end of life"),
            "eol_add_med_record_yn" -> Array("1")
          )
        )
      )

    mapped shouldBe None
  }

}
