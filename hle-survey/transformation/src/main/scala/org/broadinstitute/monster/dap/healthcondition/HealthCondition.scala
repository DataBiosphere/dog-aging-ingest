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
  * @param descriptionSuffixOverride if set, will override the auto-computed field name
  *                                  for the description field on "other" conditions
  */
sealed abstract class HealthCondition(
  override val value: Long,
  val label: String,
  val conditionType: HealthConditionType,
  val both: Option[String] = None,
  val cg: Option[String] = None,
  val dx: Option[String] = None,
  val isOther: Boolean = false,
  val descriptionSuffixOverride: Option[String] = None
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
  case object AS extends HealthCondition(501L, "Aortic/Subaortic stenosis", Cardiac, cg = Some("as"))
  case object ASD extends HealthCondition(502L, "Atrial septal defects", Cardiac, cg = Some("asd"))
  case object MitralDysplasia extends HealthCondition(503L, "Mitral dysplasia", Cardiac, cg = Some("mit_dys"))
  case object Murmur extends HealthCondition(504L, "Murmur", Cardiac, cg = Some("murmur"), dx = Some("mur"))
  case object PDA extends HealthCondition(505L, "Patent ductus arteriosus (PDA)", Cardiac, cg = Some("pda"))
  case object PRAA extends HealthCondition(506L, "Persistent right aortic arch", Cardiac, cg = Some("praa"))
  case object PulmonicStenosis extends HealthCondition(507L, "Pulmonic stenosis", Cardiac, cg = Some("p_steno"), dx = Some("ps"))
  case object TricuspidDysplasia extends HealthCondition(508L, "Tricuspid dysplasia", Cardiac, cg = Some("tri_dys"))
  case object VSD extends HealthCondition(509L, "Ventricular septal defects", Cardiac, cg = Some("vsd"))
  case object Arrhythmia extends HealthCondition(510L, "Arrhythmia", Cardiac, dx = Some("arr"))
  case object Cardiomyopathy extends HealthCondition(511L, "Cardiomyopathy", Cardiac, dx = Some("car"))
  case object CHF extends HealthCondition(512L, "Congestive heart failure", Cardiac, dx = Some("chf"))
  case object Endocarditis extends HealthCondition(513L, "Endocarditis", Cardiac, dx = Some("end"))
  case object Hypertension extends HealthCondition(514L, "Hypertension (high blood pressure)", Cardiac, dx = Some("hbp"))
  case object PericardialEffusion extends HealthCondition(515L, "Pericardial effusion", Cardiac, dx = Some("pe"))
  case object PulmonaryHypertension extends HealthCondition(516L, "Pulmonary hypertension", Cardiac, dx = Some("ph"))
  case object SubaorticStenosis extends HealthCondition(518L, "Subaortic stenosis", Cardiac, dx = Some("ss"))
  case object ValveDisease
      extends HealthCondition(519L, "Valve disease", Cardiac, dx = Some("vd"), isOther = true, descriptionSuffixOverride = Some("valve"))
  case object OtherCardiac extends HealthCondition(598L, "Other", Cardiac, both = Some("other"), isOther = true)

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
  case object LiverPS extends HealthCondition(801L, "Portosystemic shunt", Liver, both = Some("ps"))
  case object BO extends HealthCondition(802L, "Biliary obstruction", Liver, dx = Some("bo"))
  case object CILD extends HealthCondition(803L, "Chronic inflammatory liver disease", Liver, dx = Some("cild"))
  case object EPI extends HealthCondition(804L, "Exocrine pancreatic insufficiency (EPI)", Liver, dx = Some("epi"))
  case object GBM extends HealthCondition(805L, "Gall bladder mucocele", Liver, dx = Some("gbm"))
  case object GBR extends HealthCondition(806L, "Gall bladder rupture", Liver, dx = Some("gbr"))
  case object GBS extends HealthCondition(807L, "Gall bladder surgery", Liver, dx = Some("gbs"))
  case object LiverMD extends HealthCondition(808L, "Microvascular dysplasia (portal vein hypoplasia)", Liver, dx = Some("md"))
  case object Pancreatitis extends HealthCondition(809L, "Pancreatitis", Liver, dx = Some("pan"))
  case object OtherLiver extends HealthCondition(898L, "Other liver condition", Liver, both = Some("other"), isOther = true)

  // Kidney conditions.
  case object OneKidney extends HealthCondition(901L, "Born with one kidney", Kidney, cg = Some("one_kid"))
  case object EctopicUreter extends HealthCondition(902L, "Ectopic ureter", Kidney, both = Some("eu"))
  case object PatentUrachus extends HealthCondition(903L, "Patent urachus", Kidney, cg = Some("pu"))
  case object RenalCysts extends HealthCondition(904L, "Renal cysts", Kidney, cg = Some("rc"))
  case object RenalDysplasia extends HealthCondition(905L, "Renal dysplasia", Kidney, both = Some("rd"))
  case object AKF extends HealthCondition(906L, "Acute kidney failure", Kidney, dx = Some("akf"))
  case object BladderProlapse extends HealthCondition(907L, "Bladder prolapse", Kidney, dx = Some("bp"))
  case object CKD extends HealthCondition(908L, "Chronic kidney disease", Kidney, dx = Some("ckd"))
  case object Pyelonephritis extends HealthCondition(909L, "Pyelonephritis (kidney infection)", Kidney, dx = Some("ki"))
  case object KidneyStones extends HealthCondition(910L, "Kidney stones", Kidney, dx = Some("ks"))
  case object Proteinuria extends HealthCondition(911L, "Proteinuria", Kidney, dx = Some("pro"))
  case object TubularDisorder extends HealthCondition(912L, "Tubular disorder (such as Fanconi syndrome)", Kidney, dx = Some("td"))
  case object UrethralProlapse extends HealthCondition(913L, "Urethral prolapse", Kidney, dx = Some("up"))
  case object UrinaryCrystals extends HealthCondition(914L, "Urinary crystals or stones in bladder or urethra", Kidney, dx = Some("ub"))
  case object UrinaryIncontinence extends HealthCondition(915L, "Urinary incontinence", Kidney, dx = Some("ui"))
  case object UTI extends HealthCondition(916L, "Urinary tract infection (chronic or recurrent)", Kidney, dx = Some("uti"))
  case object OtherKidney extends HealthCondition(998L, "Other kidney condition", Kidney, both = Some("other"), isOther = true)

  // Reproductive conditions.
  case object Cryptorchid extends HealthCondition(1001L, "Cryptorchid", Reproductive, cg = Some("crypto"))
  case object Hermaphroditism extends HealthCondition(1002L, "Hermaphroditism", Reproductive, cg = Some("herma"))
  case object Hypospadias extends HealthCondition(1003L, "Hypospadias", Reproductive, cg = Some("hypo"))
  case object Phimosis extends HealthCondition(1004L, "Phimosis", Reproductive, cg = Some("phimo"))
  case object BPH extends HealthCondition(1005L, "Benign prostatic hyperplasia", Reproductive, dx = Some("bph"))
  case object Dystocia extends HealthCondition(1006L, "Dystocia", Reproductive, dx = Some("dys"))
  case object IHC extends HealthCondition(1007L, "Irregular heat cycle", Reproductive, dx = Some("ihc"))
  case object Mastitis extends HealthCondition(1008L, "Mastitis", Reproductive, dx = Some("mas"))
  case object Papilloma extends HealthCondition(1009L, "Papilloma (genital warts)", Reproductive, dx = Some("pgw"))
  case object Paraphimosis extends HealthCondition(1010L, "Paraphimosis", Reproductive, dx = Some("para"))
  case object Prostatitis extends HealthCondition(1011L, "Prostatitis", Reproductive, dx = Some("pros"))
  case object PreputialInfection extends HealthCondition(1012L, "Preputial infection", Reproductive, dx = Some("pi"))
  case object Pseudopregnancy extends HealthCondition(1013L, "Pseudopregnancy", Reproductive, dx = Some("pse"))
  case object Pyometra extends HealthCondition(1014L, "Pyometra", Reproductive, dx = Some("pyo"))
  case object RecessedVulva extends HealthCondition(1015L, "Recessed vulva", Reproductive, dx = Some("rv"))
  case object TesticularAtrophy extends HealthCondition(1016L, "Testicular atrophy", Reproductive, dx = Some("ta"))
  case object Vaginitis extends HealthCondition(1017L, "Vaginitis", Reproductive, dx = Some("vag"))
  case object OtherReproductive extends HealthCondition(1098L, "Other reproductive condition", Reproductive, both = Some("other"), isOther = true)

  // Orthopedic conditions.
  case object MissingLimb extends HealthCondition(1101L, "Missing a limb or part of a limb", Orthopedic, cg = Some("limb"))
  case object ValgusDeformity extends HealthCondition(1102L, "Valgus deformity", Orthopedic, cg = Some("valgus"))
  case object VarusDeformity extends HealthCondition(1103L, "Varus deformity", Orthopedic, cg = Some("varus"))
  case object CSS extends HealthCondition(1104L, "Carpal subluxation syndrome", Orthopedic, dx = Some("css"))
  case object CLR extends HealthCondition(1105L, "Cruciate ligament rupture", Orthopedic, dx = Some("clr"))
  case object DJD extends HealthCondition(1106L, "Degenerative joint disease", Orthopedic, dx = Some("djd"))
  case object Dwarfism extends HealthCondition(1107L, "Dwarfism", Orthopedic, dx = Some("dwa"))
  case object ElbowDysplasia extends HealthCondition(1108L, "Elbow dysplasia", Orthopedic, dx = Some("ed"))
  case object GrowthDeformity extends HealthCondition(1109L, "Growth deformity", Orthopedic, dx = Some("gd"))
  case object HipDysplasia extends HealthCondition(1110L, "Hip dysplasia", Orthopedic, dx = Some("hd"))
  case object IVDD extends HealthCondition(1111L, "Intervertebral disc disease (IVDD)", Orthopedic, dx = Some("ivdd"))
  case object Lameness extends HealthCondition(1112L, "Lameness (chronic or recurrent)", Orthopedic, dx = Some("lame"))
  case object Osteoarthritis extends HealthCondition(1113L, "Osteoarthritis", Orthopedic, dx = Some("oa"))
  case object OCD extends HealthCondition(1114L, "Osteochondritis dissecans (OCD)", Orthopedic, dx = Some("ocd"))
  case object Osteomyelitis extends HealthCondition(1115L, "Osteomyelitis", Orthopedic, dx = Some("om"))
  case object Panosteitis extends HealthCondition(1116L, "Panosteitis", Orthopedic, dx = Some("pano"))
  case object PatellarLuxation extends HealthCondition(1117L, "Patellar luxation", Orthopedic, dx = Some("pl"))
  case object RheumatoidArthritis extends HealthCondition(1118L, "Rheumatoid arthritis", Orthopedic, dx = Some("ra"))
  case object Spondylosis extends HealthCondition(1119L, "Spondylosis", Orthopedic, dx = Some("spo"))
  case object OtherOrthopedic extends HealthCondition(1198L, "Other orthopedic condition", Orthopedic, both = Some("other"), isOther = true)

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
