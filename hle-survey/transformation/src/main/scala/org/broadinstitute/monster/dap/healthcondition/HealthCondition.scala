package org.broadinstitute.monster.dap.healthcondition

import enumeratum.values.{LongEnum, LongEnumEntry}

/**
  * Specific health condition that a dog might have experienced.
  *
  * @param value raw value to store on a per-row basis in BQ
  * @param label string label to associate with the raw value in lookup tables
  * @param conditionType general category of the condition
  * @param both abbreviation used for both the 'cg' and 'dx' representations
  *             of this condition, if one exists
  * @param cg abbreviation used for the 'cg' representation of this condition, if one exists
  * @param dx abbreviation used for the 'dx' representation of this condition, if one exists
  * @param isOther true if the condition has a '_spec' field for free-form user entry
  */
sealed abstract class HealthCondition(
  override val value: Long,
  val label: String,
  val conditionType: HealthConditionType,
  val both: Option[String] = None,
  val cg: Option[String] = None,
  val dx: Option[String] = None,
  val isOther: Boolean = false
) extends LongEnumEntry

object HealthCondition extends LongEnum[HealthCondition] {
  override val values = findValues

  val cgValues = values.filter(c => c.both.isDefined || c.cg.isDefined)
  val dxValues = values.filter(c => c.both.isDefined || c.dx.isDefined)

  import HealthConditionType.{findValues => _, _}

  // scalafmt: { maxColumn = 145, newlines.topLevelStatements = [] }

  // Eye conditions.
  case object Blindness extends HealthCondition(101L, "Blindness", Eye, both = Some("blind"))
  case object Cataracts extends HealthCondition(102L, "Cataracts", Eye, both = Some("cat"))
  case object Glaucoma extends HealthCondition(103L, "Glaucoma", Eye, both = Some("glauc"))
  case object KCS extends HealthCondition(104L, "Keratoconjunctivitis sicca (KCS)", Eye, both = Some("kcs"))
  case object PPM extends HealthCondition(105L, "Persistent pupillary membrane (PPM)", Eye, cg = Some("ppm"))
  case object MissingEye extends HealthCondition(106L, "Missing one or both eyes", Eye, cg = Some("miss"))
  case object CherryEye extends HealthCondition(107L, "Third eyelid prolapse (cherry eye)", Eye, dx = Some("ce"))
  case object Conjunctivitis extends HealthCondition(108L, "Conjunctivitis", Eye, dx = Some("conj"))
  case object CornealUlcer extends HealthCondition(109L, "Corneal ulcer", Eye, dx = Some("cu"))
  case object Distichia extends HealthCondition(110L, "Distichia", Eye, dx = Some("dist"))
  case object Ectropion extends HealthCondition(111L, "Ectropion (eyelid rolled out)", Eye, dx = Some("ectrop"))
  case object Entropion extends HealthCondition(112L, "Entropion (eyelid rolled in)", Eye, dx = Some("entrop"))
  case object ILP extends HealthCondition(113L, "Imperforate lacrimal punctum", Eye, dx = Some("ilp"))
  case object IrisCyst extends HealthCondition(114L, "Iris cyst", Eye, dx = Some("ic"))
  case object JuvenileCataracts extends HealthCondition(115L, "Juvenile cataracts", Eye, dx = Some("jcat"))
  case object NS extends HealthCondition(116L, "Nuclear sclerosis", Eye, dx = Some("ns"))
  case object PU extends HealthCondition(117L, "Pigmentary uveitis", Eye, dx = Some("pu"))
  case object PRA extends HealthCondition(118L, "Progressive retinal atrophy", Eye, dx = Some("pra"))
  case object RD extends HealthCondition(119L, "Retinal detachment", Eye, dx = Some("rd"))
  case object Uveitis extends HealthCondition(120L, "Uveitis", Eye, dx = Some("uvei"))
  case object OtherEye extends HealthCondition(198L, "Other eye condition", Eye, cg = Some("other"), dx = Some("eye_other"), isOther = true)

  // Ear conditions.
  case object Deafness extends HealthCondition(201L, "Deafness", Ear, both = Some("deaf"))
  case object EarInfection extends HealthCondition(202L, "Ear Infection", Ear, dx = Some("ei"))
  case object EarMites extends HealthCondition(203L, "Ear Mites", Ear, dx = Some("em"))
  case object Epistaxis extends HealthCondition(204L, "Epistaxis (nose bleeds)", Ear, dx = Some("epis"))
  case object HearingLoss extends HealthCondition(205L, "Hearing loss (incompletely deaf)", Ear, dx = Some("hl"))
  case object Hematoma extends HealthCondition(206L, "Hematoma", Ear, dx = Some("hemato"))
  case object Pharyngitis extends HealthCondition(207L, "Pharyngitis", Ear, dx = Some("phary"))
  case object Rhinitis extends HealthCondition(208L, "Rhinitis", Ear, dx = Some("rhini"))
  case object Tonsillitis extends HealthCondition(209L, "Tonsillitis", Ear, dx = Some("tonsi"))
  case object OtherEar extends HealthCondition(298L, "Other ear condition", Ear, both = Some("other"), isOther = true)

  // Oral conditions.
  // TODO

  // Skin conditions.
  case object DermoidCysts extends HealthCondition(401L, "Dermoid cysts", Skin, cg = Some("dcysts"))
  case object SpinaBifida extends HealthCondition(402L, "Spina bifida", Skin, cg = Some("sp_bif"))
  case object UmbilicalHernia extends HealthCondition(403L, "Umbilical hernia", Skin, cg = Some("uh"))
  case object Alopecia extends HealthCondition(404L, "Alopecia (hair loss)", Skin, dx = Some("alo"))
  case object AtopicDermatitis extends HealthCondition(405L, "Atopic dermatitis (atopy)", Skin, dx = Some("ad"))
  case object ChronicHotSpots extends HealthCondition(406L, "Chronic or recurrent hot spots", Skin, dx = Some("chs"))
  case object ChronicSkinInfections extends HealthCondition(407L, "Chronic or recurrent skin infections", Skin, dx = Some("csi"))
  case object ContactDermatitis extends HealthCondition(408L, "Contact dermatitis", Skin, dx = Some("cd"))
  case object DLE extends HealthCondition(409L, "Discoid lupus erythematosus (DLE)", Skin, dx = Some("dle"))
  case object FAD extends HealthCondition(410L, "Flea allergy dermatitis", Skin, dx = Some("fad"))
  case object Fleas extends HealthCondition(411L, "Fleas", Skin, dx = Some("flea"))
  case object FMA extends HealthCondition(412L, "Food or medicine allergies that affect the skin", Skin, dx = Some("fma"))
  case object Ichthyosis extends HealthCondition(413L, "Ichthyosis", Skin, dx = Some("ich"))
  case object LickGranuloma extends HealthCondition(414L, "Lick granuloma", Skin, dx = Some("lg"))
  case object NSD extends HealthCondition(415L, "Non-specific dermatosis", Skin, dx = Some("nsd"))
  case object PPP extends HealthCondition(416L, "Panepidermal pustular pemphigus (PPP)", Skin, dx = Some("ppp"))
  case object PNP extends HealthCondition(417L, "Paraneoplastic pemphigus (PNP)", Skin, dx = Some("pnp"))
  case object PE extends HealthCondition(418L, "Pemphigus erythematosus (PE)", Skin, dx = Some("pe"))
  case object PF extends HealthCondition(419L, "Pemphigus foliaceus (PF)", Skin, dx = Some("pf"))
  case object PV extends HealthCondition(420L, "Pemphigus vulgaris (PV)", Skin, dx = Some("pv"))
  case object Pododermatitis extends HealthCondition(421L, "Pododermatitis", Skin, dx = Some("podo"))
  case object Polymyositis extends HealthCondition(422L, "Polymyositis", Skin, dx = Some("poly"))
  case object Pruritis extends HealthCondition(423L, "Pruritis (itchy skin)", Skin, dx = Some("pru"))
  case object Pyoderma extends HealthCondition(424L, "Pyoderma or bacterial dermatitis", Skin, dx = Some("pyo"))
  case object SarcopticMange extends HealthCondition(425L, "Sarcoptic mange", Skin, dx = Some("sm"))
  case object SeasonalAllergies extends HealthCondition(426L, "Seasonal allergies", Skin, dx = Some("sall"))
  case object SebaceousAdenitis extends HealthCondition(427L, "Sebaceous adenitis", Skin, dx = Some("sade"))
  case object SebaceousCysts extends HealthCondition(428L, "Sebaceous cysts", Skin, dx = Some("scys"))
  case object Seborrhea extends HealthCondition(429L, "Seborrhea or seborrheic dermatitis (greasy skin)", Skin, dx = Some("sd"))
  case object SDM extends HealthCondition(430L, "Systemic demodectic mange", Skin, dx = Some("sdm"))
  case object SLE extends HealthCondition(431L, "Systemic lupus erythematosus (SLE)", Skin, dx = Some("sle"))
  case object Ticks extends HealthCondition(432L, "Ticks", Skin, dx = Some("tick"))
  case object OtherSkin extends HealthCondition(498L, "Other skin condition", Skin, both = Some("other"), isOther = true)

  // Cardiac conditions.
  // TODO

  // Respiratory conditions.
  case object SNN extends HealthCondition(601L, "Stenotic/narrow nares", Respiratory, cg = Some("st_nares"), dx = Some("snn"))
  case object TS extends HealthCondition(602L, "Tracheal stenosis (narrowing)", Respiratory, cg = Some("tr_steno"), dx = Some("ts"))
  case object ARDS extends HealthCondition(603L, "Acquired or acute respiratory distress syndrome (ARDS)", Respiratory, dx = Some("ards"))
  case object ChronicBronchitis extends HealthCondition(604L, "Chronic or recurrent bronchitis", Respiratory, dx = Some("cb"))
  case object ChronicCough extends HealthCondition(605L, "Chronic or recurrent cough", Respiratory, dx = Some("cc"))
  case object ChronicRhinitis extends HealthCondition(606L, "Chronic or recurrent rhinitis", Respiratory, dx = Some("cr"))
  case object ESP extends HealthCondition(607L, "Elongated soft palate", Respiratory, dx = Some("esp"))
  case object LP extends HealthCondition(608L, "Laryngeal paralysis", Respiratory, dx = Some("lp"))
  case object LLT extends HealthCondition(609L, "Lung lobe torsion", Respiratory, dx = Some("llt"))
  case object Pneumonia extends HealthCondition(610L, "Pneumonia", Respiratory, dx = Some("pn"))
  case object PulmonaryBullae extends HealthCondition(611L, "Pulmonary bullae", Respiratory, dx = Some("pul"))
  case object TrachealCollapse extends HealthCondition(612L, "Tracheal collapse", Respiratory, dx = Some("tc"))
  case object OtherRespiratory extends HealthCondition(698L, "Other respiratory condition", Respiratory, both = Some("other"), isOther = true)

  // Gastrointestinal conditions.
  case object Atresia extends HealthCondition(701L, "Atresia ani", Gastrointestinal, cg = Some("atresia"))
  case object EA extends HealthCondition(702L, "Esophageal achalasia", Gastrointestinal, cg = Some("ea"))
  case object Megaesophagus extends HealthCondition(703L, "Megaesophagus", Gastrointestinal, cg = Some("megaeso"), dx = Some("meg"))
  case object UH extends HealthCondition(704L, "Umbilical hernia", Gastrointestinal, cg = Some("uh"))
  case object ASI extends HealthCondition(705L, "Anal sac impaction", Gastrointestinal, dx = Some("asi"))
  case object BVS extends HealthCondition(706L, "Bilious vomiting syndrome", Gastrointestinal, dx = Some("bvs"))
  case object GDV extends HealthCondition(707L, "Bloat with torsion (GDV)", Gastrointestinal, dx = Some("gdv"))
  case object CD extends HealthCondition(708L, "Chronic or recurrent diarrhea", Gastrointestinal, dx = Some("cd"))
  case object CV extends HealthCondition(709L, "Chronic or recurrent vomiting", Gastrointestinal, dx = Some("cv"))
  case object Constipation extends HealthCondition(710L, "Constipation", Gastrointestinal, dx = Some("con"))
  case object FI extends HealthCondition(711L, "Fecal incontinence", Gastrointestinal, dx = Some("fi"))
  case object FoodAllergy extends HealthCondition(712L, "Food or medicine allergies", Gastrointestinal, dx = Some("fma"))
  case object FBIB extends HealthCondition(713L, "Foreign body ingestion or blockage", Gastrointestinal, dx = Some("fbib"))
  case object HGE extends HealthCondition(714L, "Hemorrhagic gastroenteritis (HGE) or stress colitis (acute)", Gastrointestinal, dx = Some("hge"))
  case object ICC extends HealthCondition(715L, "Idiopathic canine colitis (chronic)", Gastrointestinal, dx = Some("icc"))
  case object IBS
      extends HealthCondition(716L, "Irritable bowel syndrome (IBS) or inflammatory bowel disease (IBD)", Gastrointestinal, dx = Some("ibd"))
  case object Lymphangiectasia extends HealthCondition(717L, "Lymphangiectasia", Gastrointestinal, dx = Some("lym"))
  case object MD extends HealthCondition(718L, "Malabsorptive disorder", Gastrointestinal, dx = Some("md"))
  case object OtherAllergy extends HealthCondition(719L, "Other allergies", Gastrointestinal, dx = Some("all"))
  case object PLE extends HealthCondition(720L, "Protein-losing enteropathy (PLE)", Gastrointestinal, dx = Some("ple"))
  case object PS extends HealthCondition(721L, "Pyloric stenosis", Gastrointestinal, dx = Some("ps"))
  case object OtherGI extends HealthCondition(798L, "Other gastrointestinal condition", Gastrointestinal, both = Some("other"), isOther = true)

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
  case object OtherCG extends HealthCondition(1598L, "Other congenital disorder", OtherCongenital, cg = Some("other"), isOther = true)

  // Infections diseases.
  case object Anaplasmosis extends HealthCondition(1601L, "Anaplasmosis", Infection, dx = Some("anaplasmosis"))
  case object Aspergillosis extends HealthCondition(1602L, "Aspergillosis", Infection, dx = Some("asperg"))
  case object Babesiosis extends HealthCondition(1603L, "Babesiosis", Infection, dx = Some("babesio"))
  case object Blastomycosis extends HealthCondition(1604L, "Blastomycosis", Infection, dx = Some("blastomy"))
  case object Bordetella
      extends HealthCondition(1605L, """Bordetella and/or parainfluenza ("kennel cough")""", Infection, dx = Some("bordetella"))
  case object Brucellosis extends HealthCondition(1606L, "Brucellosis", Infection, dx = Some("brucellosis"))
  case object Campylobacteriosis extends HealthCondition(1607L, "Campylobacteriosis", Infection, dx = Some("campylo"))
  case object Chagas extends HealthCondition(1608L, "Chagas disease (trypanosomiasis)", Infection, dx = Some("chagas"))
  case object Coccidia extends HealthCondition(1609L, "Coccidia", Infection, dx = Some("ccdia"))
  case object Coccidioidiomycosis extends HealthCondition(1610L, "Coccidioidiomycosis", Infection, dx = Some("ccdio"))
  case object Cryptococcus extends HealthCondition(1611L, "Cryptococcus", Infection, dx = Some("crypto"))
  case object Ringworm extends HealthCondition(1612L, """Dermatophytosis ("ringworm")""", Infection, dx = Some("dermato"))
  case object Distemper extends HealthCondition(1613L, "Distemper", Infection, dx = Some("dstmp"))
  case object Ehrlichiosis extends HealthCondition(1614L, "Ehrlichiosis", Infection, dx = Some("ehrlich"))
  case object Fever extends HealthCondition(1615L, "Fever of unknown origin", Infection, dx = Some("fever"))
  case object GastroParasites extends HealthCondition(1616L, "Gastrointestinal parasites", Infection, dx = Some("gp"))
  case object Giardia extends HealthCondition(1617L, "Giardia", Infection, dx = Some("giar"))
  case object Granuloma extends HealthCondition(1618L, "Granuloma", Infection, dx = Some("granu"))
  case object Heartworms extends HealthCondition(1619L, "Heartworm infection", Infection, dx = Some("hrtworm"))
  case object Histoplasmosis extends HealthCondition(1620L, "Histoplasmosis", Infection, dx = Some("histo"))
  case object Hepatozoonosis extends HealthCondition(1621L, "Hepatozoonosis", Infection, dx = Some("hepato"))
  case object Hookworms extends HealthCondition(1622L, "Hookworms", Infection, dx = Some("hkworm"))
  case object Influenza extends HealthCondition(1623L, "Influenza", Infection, dx = Some("influ"))
  case object Isospora extends HealthCondition(1624L, "Isospora", Infection, dx = Some("isosp"))
  case object Leishmaniasis extends HealthCondition(1625L, "Leishmaniasis", Infection, dx = Some("leish"))
  case object Leptospirosis extends HealthCondition(1626L, "Leptospirosis", Infection, dx = Some("lepto"))
  case object Lyme extends HealthCondition(1627L, "Lyme disease", Infection, dx = Some("lyme"))
  case object MRSA extends HealthCondition(1628L, "MRSA/MRSP", Infection, dx = Some("mrsa"))
  case object Mycobacterium extends HealthCondition(1629L, "Mycobacterium", Infection, dx = Some("mycob"))
  case object Parvovirus extends HealthCondition(1630L, "Parvovirus", Infection, dx = Some("parvo"))
  case object Plague extends HealthCondition(1631L, "Plague (Yersinia pestis)", Infection, dx = Some("plague"))
  case object Pythium extends HealthCondition(1632L, "Pythium", Infection, dx = Some("pythium"))
  case object RMSF extends HealthCondition(1633L, "Rocky Mountain Spotted Fever (RMSF)", Infection, dx = Some("rmsf"))
  case object Roundworms extends HealthCondition(1634L, "Roundworms", Infection, dx = Some("rndworm"))
  case object Salmonellosis extends HealthCondition(1635L, "Salmonellosis", Infection, dx = Some("slmosis"))
  case object SalmonPoison extends HealthCondition(1636L, "Salmon poisoning", Infection, dx = Some("slmpois"))
  case object Tapeworms extends HealthCondition(1637L, "Tapeworms", Infection, dx = Some("tpworm"))
  case object Toxoplasma extends HealthCondition(1638L, "Toxoplasma", Infection, dx = Some("toxop"))
  case object Tularemia extends HealthCondition(1639L, "Tularemia", Infection, dx = Some("tular"))
  case object Whipworms extends HealthCondition(1640L, "Whipworms", Infection, dx = Some("whpworm"))
  case object OtherInfection extends HealthCondition(1698L, "Other infectious disease", Infection, dx = Some("infect_other"), isOther = true)

  // Toxin consumption.
  // TODO

  // Trauma.
  case object DogBite extends HealthCondition(1801L, "Dog bite", Trauma, dx = Some("dogbite"))
  case object AnimalBite extends HealthCondition(1802L, "Bite wound from another animal", Trauma, dx = Some("anibite"))
  case object Fall extends HealthCondition(1803L, "Fall from height", Trauma, dx = Some("fall"))
  case object Fracture extends HealthCondition(1804L, "Fractured bone", Trauma, dx = Some("frac"))
  case object Head extends HealthCondition(1805L, "Head trauma due to any cause", Trauma, dx = Some("head"))
  case object Car extends HealthCondition(1806L, "Hit by car or other vehicle", Trauma, dx = Some("car"))
  case object Kick extends HealthCondition(1807L, "Kicked by horse or other large animal", Trauma, dx = Some("kick"))
  case object Laceration extends HealthCondition(1808L, "Laceration", Trauma, dx = Some("lac"))
  case object PenetratingWound extends HealthCondition(1809L, "Penetrating wound (such as a stick)", Trauma, dx = Some("pene"))
  case object Proptosis extends HealthCondition(1810L, "Proptosis (eye out of socket)", Trauma, dx = Some("prop"))
  case object SnakeBite extends HealthCondition(1811L, "Snakebite", Trauma, dx = Some("snake"))
  case object Tail extends HealthCondition(1812L, "Tail injury", Trauma, dx = Some("tail"))
  case object Nail extends HealthCondition(1813L, "Torn or broken toenail", Trauma, dx = Some("nail"))
  case object OtherTrauma extends HealthCondition(1898L, "Other trauma", Trauma, dx = Some("other"), isOther = true)

  // Immune conditions.
  // TODO
}
