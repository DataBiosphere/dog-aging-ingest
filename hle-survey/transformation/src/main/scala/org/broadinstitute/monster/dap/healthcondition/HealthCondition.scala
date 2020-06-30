package org.broadinstitute.monster.dap.healthcondition

import enumeratum.values.{LongEnum, LongEnumEntry}

/**
  * Specific health condition that a dog might have experienced.
  *
  * @param value raw value to store on a per-row basis in BQ
  * @param label string label to associate with the raw value in lookup tables
  * @param conditionType general category of the condition
  * @param abbreviation short-hand used in RedCap to represent the condition
  * @param hasCg true if the RedCap data might contain 'cg' data for the condition
  * @param hasDx true if the RedCap data might contain 'dx' data for the condition
  * @param isOther true if the condition has a '_spec' field for free-form user entry
  * @param cgPrefixOverride if set, will override the auto-computed value used for
  *                         the prefix of congenital data fields
  * @param computeGate mapping function from `data prefix` => the name of the Y/N field
  *                    marking if any data exists for the condition
  */
sealed abstract class HealthCondition(
  override val value: Long,
  val label: String,
  val conditionType: HealthConditionType,
  val abbreviation: String,
  val hasCg: Boolean,
  val hasDx: Boolean,
  val isOther: Boolean = false,
  val cgPrefixOverride: Option[String] = None,
  val computeGate: String => String = identity
) extends LongEnumEntry

object HealthCondition extends LongEnum[HealthCondition] {
  override val values = findValues

  val cgValues = values.filter(_.hasCg)
  val dxValues = values.filter(_.hasDx)

  import HealthConditionType.{findValues => _, _}

  // scalafmt: { maxColumn = 140, newlines.topLevelStatements = [] }

  // Eye conditions.
  case object Blindness extends HealthCondition(101L, "Blindness", Eye, "blind", true, true)
  case object Cataracts extends HealthCondition(102L, "Cataracts", Eye, "cat", true, true)
  case object Glaucoma extends HealthCondition(103L, "Glaucoma", Eye, "glauc", true, true)
  case object KCS extends HealthCondition(104L, "Keratoconjunctivitis sicca (KCS)", Eye, "kcs", true, true)
  case object PPM extends HealthCondition(105L, "Persistent pupillary membrane (PPM)", Eye, "ppm", true, false)
  case object MissingEye extends HealthCondition(106L, "Missing one or both eyes", Eye, "miss", true, false)
  case object CherryEye extends HealthCondition(108L, "Third eyelid prolapse (cherry eye)", Eye, "ce", false, true)
  case object Conjunctivitis extends HealthCondition(109L, "Conjunctivitis", Eye, "conj", false, true)
  case object CornealUlcer extends HealthCondition(110L, "Corneal ulcer", Eye, "cu", false, true)
  case object Distichia extends HealthCondition(111L, "Distichia", Eye, "dist", false, true)
  case object Ectropion extends HealthCondition(112L, "Ectropion (eyelid rolled out)", Eye, "ectrop", false, true)
  case object Entropion extends HealthCondition(113L, "Entropion (eyelid rolled in)", Eye, "entrop", false, true)
  case object ILP extends HealthCondition(114L, "Imperforate lacrimal punctum", Eye, "ilp", false, true)
  case object IrisCyst extends HealthCondition(115L, "Iris cyst", Eye, "ic", false, true)
  case object JuvenileCataracts extends HealthCondition(116L, "Juvenile cataracts", Eye, "jcat", false, true)
  case object NS extends HealthCondition(117L, "Nuclear sclerosis", Eye, "ns", false, true)
  case object PU extends HealthCondition(118L, "Pigmentary uveitis", Eye, "pu", false, true)
  case object PRA extends HealthCondition(119L, "Progressive retinal atrophy", Eye, "pra", false, true)
  case object RD extends HealthCondition(120L, "Retinal detachment", Eye, "rd", false, true)
  case object Uveitis extends HealthCondition(121L, "Uveitis", Eye, "uvei", false, true)
  case object OtherEye extends HealthCondition(198L, "Other eye condition", Eye, "eye_other", true, true, true, Some("hs_cg_eye_other"))

  // Ear conditions.
  case object Deafness extends HealthCondition(201L, "Deafness", Ear, "deaf", true, true)
  case object EarInfection extends HealthCondition(202L, "Ear Infection", Ear, "ei", false, true)
  case object EarMites extends HealthCondition(203L, "Ear Mites", Ear, "em", false, true)
  case object Epistaxis extends HealthCondition(204L, "Epistaxis (nose bleeds)", Ear, "epis", false, true)
  case object HearingLoss extends HealthCondition(205L, "Hearing loss (incompletely deaf)", Ear, "hl", false, true)
  case object Hematoma extends HealthCondition(206L, "Hematoma", Ear, "hemato", false, true)
  case object Pharyngitis extends HealthCondition(207L, "Pharyngitis", Ear, "phary", false, true)
  case object Rhinitis extends HealthCondition(208L, "Rhinitis", Ear, "rhini", false, true)
  case object Tonsillitis extends HealthCondition(209L, "Tonsillitis", Ear, "tonsi", false, true)
  case object OtherEar extends HealthCondition(298L, "Other ear condition", Ear, "other", true, true, true, Some("hs_cg_ear_other"))

  // Oral conditions.
  // TODO

  // Skin conditions.
  // TODO

  // Cardiac conditions.
  // TODO

  // Respiratory conditions.
  // TODO

  // Gastrointestinal conditions.
  // TODO

  // Liver conditions.
  // TODO

  // Kidney conditions.
  // TODO

  // Reproductive conditions.
  // TODO

  // Orthopedic conditions.
  // TODO

  // Neurologic conditions.
  // TODO

  // Endocrine conditions.
  // TODO

  // Hematopoietic conditions.
  // TODO

  // Other congenital conditions (annoying one-off case).
  case object OtherCG
      extends HealthCondition(
        1598L,
        "Other congenital disorder",
        OtherCongenital,
        "other",
        true,
        false,
        true,
        Some("hs_cg_other"),
        prefix => s"${prefix}_yn"
      )

  // Infections diseases.
  case object Anaplasmosis extends HealthCondition(1601L, "Anaplasmosis", Infection, "anaplasmosis", false, true)
  case object Aspergillosis extends HealthCondition(1602L, "Aspergillosis", Infection, "asperg", false, true)
  case object Babesiosis extends HealthCondition(1603L, "Babesiosis", Infection, "babesio", false, true)
  case object Blastomycosis extends HealthCondition(1604L, "Blastomycosis", Infection, "blastomy", false, true)
  case object Bordetella
      extends HealthCondition(1605L, """Bordetella and/or parainfluenza ("kennel cough")""", Infection, "bordetella", false, true)
  case object Brucellosis extends HealthCondition(1606L, "Brucellosis", Infection, "brucellosis", false, true)
  case object Campylobacteriosis extends HealthCondition(1607L, "Campylobacteriosis", Infection, "campylo", false, true)
  case object Chagas extends HealthCondition(1608L, "Chagas disease (trypanosomiasis)", Infection, "chagas", false, true)
  case object Coccidia extends HealthCondition(1609L, "Coccidia", Infection, "ccdia", false, true)
  case object Coccidioidiomycosis extends HealthCondition(1610L, "Coccidioidiomycosis", Infection, "ccdio", false, true)
  case object Cryptococcus extends HealthCondition(1611L, "Cryptococcus", Infection, "crypto", false, true)
  case object Ringworm extends HealthCondition(1612L, """Dermatophytosis ("ringworm")""", Infection, "dermato", false, true)
  case object Distemper extends HealthCondition(1613L, "Distemper", Infection, "dstmp", false, true)
  case object Ehrlichiosis extends HealthCondition(1614L, "Ehrlichiosis", Infection, "ehrlich", false, true)
  case object Fever extends HealthCondition(1615L, "Fever of unknown origin", Infection, "fever", false, true)
  case object GastroParasites extends HealthCondition(1616L, "Gastrointestinal parasites", Infection, "gp", false, true)
  case object Giardia extends HealthCondition(1617L, "Giardia", Infection, "giar", false, true)
  case object Granuloma extends HealthCondition(1618L, "Granuloma", Infection, "granu", false, true)
  case object Heartworms extends HealthCondition(1619L, "Heartworm infection", Infection, "hrtworm", false, true)
  case object Histoplasmosis extends HealthCondition(1620L, "Histoplasmosis", Infection, "histo", false, true)
  case object Hepatozoonosis extends HealthCondition(1621L, "Hepatozoonosis", Infection, "hepato", false, true)
  case object Hookworms extends HealthCondition(1622L, "Hookworms", Infection, "hkworm", false, true)
  case object Influenza extends HealthCondition(1623L, "Influenza", Infection, "influ", false, true)
  case object Isospora extends HealthCondition(1624L, "Isospora", Infection, "isosp", false, true)
  case object Leishmaniasis extends HealthCondition(1625L, "Leishmaniasis", Infection, "leish", false, true)
  case object Leptospirosis extends HealthCondition(1626L, "Leptospirosis", Infection, "lepto", false, true)
  case object Lyme extends HealthCondition(1627L, "Lyme disease", Infection, "lyme", false, true)
  case object MRSA extends HealthCondition(1628L, "MRSA/MRSP", Infection, "mrsa", false, true)
  case object Mycobacterium extends HealthCondition(1629L, "Mycobacterium", Infection, "mycob", false, true)
  case object Parvovirus extends HealthCondition(1630L, "Parvovirus", Infection, "parvo", false, true)
  case object Plague extends HealthCondition(1631L, "Plague (Yersinia pestis)", Infection, "plague", false, true)
  case object Pythium extends HealthCondition(1632L, "Pythium", Infection, "pythium", false, true)
  case object RMSF extends HealthCondition(1633L, "Rocky Mountain Spotted Fever (RMSF)", Infection, "rmsf", false, true)
  case object Roundworms extends HealthCondition(1634L, "Roundworms", Infection, "rndworm", false, true)
  case object Salmonellosis extends HealthCondition(1635L, "Salmonellosis", Infection, "slmosis", false, true)
  case object SalmonPoison extends HealthCondition(1636L, "Salmon poisoning", Infection, "slmpois", false, true)
  case object Tapeworms extends HealthCondition(1637L, "Tapeworms", Infection, "tpworm", false, true)
  case object Toxoplasma extends HealthCondition(1638L, "Toxoplasma", Infection, "toxop", false, true)
  case object Tularemia extends HealthCondition(1639L, "Tularemia", Infection, "tular", false, true)
  case object Whipworms extends HealthCondition(1640L, "Whipworms", Infection, "whpworm", false, true)
  case object OtherInfection
      extends HealthCondition(1698L, "Other infectious disease", Infection, "infect_other", false, true, isOther = true)

  // Toxin consumption.
  // TODO

  // Trauma.
  case object DogBite extends HealthCondition(1801L, "Dog bite", Trauma, "dogbite", false, true)
  case object AnimalBite extends HealthCondition(1802L, "Bite wound from another animal", Trauma, "anibite", false, true)
  case object Fall extends HealthCondition(1803L, "Fall from height", Trauma, "fall", false, true)
  case object Fracture extends HealthCondition(1804L, "Fractured bone", Trauma, "frac", false, true)
  case object Head extends HealthCondition(1805L, "Head trauma due to any cause", Trauma, "head", false, true)
  case object Car extends HealthCondition(1806L, "Hit by car or other vehicle", Trauma, "car", false, true)
  case object Kick extends HealthCondition(1807L, "Kicked by horse or other large animal", Trauma, "kick", false, true)
  case object Laceration extends HealthCondition(1808L, "Laceration", Trauma, "lac", false, true)
  case object PenetratingWound extends HealthCondition(1809L, "Penetrating wound (such as a stick)", Trauma, "pene", false, true)
  case object Proptosis extends HealthCondition(1810L, "Proptosis (eye out of socket)", Trauma, "prop", false, true)
  case object SnakeBite extends HealthCondition(1811L, "Snakebite", Trauma, "snake", false, true)
  case object Tail extends HealthCondition(1812L, "Tail injury", Trauma, "tail", false, true)
  case object Nail extends HealthCondition(1813L, "Torn or broken toenail", Trauma, "nail", false, true)
  case object OtherTrauma extends HealthCondition(1898L, "Other trauma", Trauma, "other", false, true, true)

  // Immune conditions.
  // TODO
}
