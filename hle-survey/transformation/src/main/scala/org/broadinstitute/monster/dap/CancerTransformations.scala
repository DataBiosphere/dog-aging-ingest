package org.broadinstitute.monster.dap

import org.broadinstitute.monster.dogaging.jadeschema.table.HlesCancerCondition

object CancerTransformations {

  /** Parse all owner-related fields out of a raw RedCap record. */
  def mapCancerConditions(rawRecord: RawRecord): HlesCancerCondition = {

    val cancerLocations = rawRecord.fields.get("hs_dx_cancer_loc")

    val cancerTypes = rawRecord.fields.get("hs_dx_cancer_type")

    val leukemiaTypes = rawRecord.fields.get("hs_dx_cancer_leuk")

    val lymphomaTypes = rawRecord.fields.get("hs_dx_cancer_lymph")

    HlesCancerCondition(
      dogId = rawRecord.getRequired("study_id").toLong,
      hsInitialDiagnosisYear = rawRecord.getOptionalNumber("hs_dx_cancer_year"),
      hsInitialDiagnosisMonth = rawRecord.getOptionalNumber("hs_dx_cancer_mo"),
      hsRequiredSurgeryOrHospitalization = rawRecord.getOptionalNumber("hs_dx_cancer_surg"),
      hsFollowUpOngoing = rawRecord.getOptionalBoolean("hs_dx_cancer_fu"),
      hsCancerLocationsAdrenalGland = cancerLocations.map(_.contains("1")),
      hsCancerLocationsAnalSac = cancerLocations.map(_.contains("2")),
      hsCancerLocationsBladderOrUrethra = cancerLocations.map(_.contains("3")),
      hsCancerLocationsBlood = cancerLocations.map(_.contains("4")),
      hsCancerLocationsBoneOrJoint = cancerLocations.map(_.contains("5")),
      hsCancerLocationsBrain = cancerLocations.map(_.contains("6")),
      hsCancerLocationsMammaryTissue = cancerLocations.map(_.contains("7")),
      hsCancerLocationsCardiacTissue = cancerLocations.map(_.contains("8")),
      hsCancerLocationsEar = cancerLocations.map(_.contains("9")),
      hsCancerLocationsEsophagus = cancerLocations.map(_.contains("10")),
      hsCancerLocationsEye = cancerLocations.map(_.contains("11")),
      hsCancerLocationsGallbladderOrBileDuct = cancerLocations.map(_.contains("12")),
      hsCancerLocationsGastrointestinalTract = cancerLocations.map(_.contains("13")),
      hsCancerLocationsKidney = cancerLocations.map(_.contains("14")),
      hsCancerLocationsLiver = cancerLocations.map(_.contains("15")),
      hsCancerLocationsLung = cancerLocations.map(_.contains("16")),
      hsCancerLocationsLymphNodes = cancerLocations.map(_.contains("17")),
      hsCancerLocationsMuscleOrSoftTissue = cancerLocations.map(_.contains("18")),
      hsCancerLocationsNoseOrNasalPassage = cancerLocations.map(_.contains("19")),
      hsCancerLocationsNerveSheath = cancerLocations.map(_.contains("20")),
      hsCancerLocationsOralCavity = cancerLocations.map(_.contains("21")),
      hsCancerLocationsOvaryOrUterus = cancerLocations.map(_.contains("22")),
      hsCancerLocationsPancreas = cancerLocations.map(_.contains("23")),
      hsCancerLocationsPerianalArea = cancerLocations.map(_.contains("24")),
      hsCancerLocationsPituitaryGland = cancerLocations.map(_.contains("25")),
      hsCancerLocationsProstate = cancerLocations.map(_.contains("26")),
      hsCancerLocationsRectum = cancerLocations.map(_.contains("27")),
      hsCancerLocationsSkinOfTrunkBodyHead = cancerLocations.map(_.contains("28")),
      hsCancerLocationsSkinOfLimbOrFoot = cancerLocations.map(_.contains("29")),
      hsCancerLocationsSpinalCord = cancerLocations.map(_.contains("30")),
      hsCancerLocationsSpleen = cancerLocations.map(_.contains("31")),
      hsCancerLocationsTesticle = cancerLocations.map(_.contains("32")),
      hsCancerLocationsThyroid = cancerLocations.map(_.contains("33")),
      hsCancerLocationsVenereal = cancerLocations.map(_.contains("34")),
      hsCancerLocationsUnknown = cancerLocations.map(_.contains("99")),
      hsCancerLocationsOther = cancerLocations.map(_.contains("98")),
      hsCancerLocationsOtherDescription =
        if (cancerLocations.getOrElse(Array.empty).contains("98")) {
          rawRecord.getOptional("hs_dx_cancer_loc_other")
        } else {
          None
        },
      hsCancerTypesAdenoma = cancerTypes.map(_.contains("1")),
      hsCancerTypesAdenocarcinoma = cancerTypes.map(_.contains("2")),
      hsCancerTypesBasalCellTumor = cancerTypes.map(_.contains("3")),
      hsCancerTypesCarcinoma = cancerTypes.map(_.contains("4")),
      hsCancerTypesChondrosarcoma = cancerTypes.map(_.contains("5")),
      hsCancerTypesCystadenoma = cancerTypes.map(_.contains("6")),
      hsCancerTypesEpidermoidCyst = cancerTypes.map(_.contains("7")),
      hsCancerTypesEpulides = cancerTypes.map(_.contains("8")),
      hsCancerTypesFibrosarcoma = cancerTypes.map(_.contains("9")),
      hsCancerTypesHemangioma = cancerTypes.map(_.contains("10")),
      hsCancerTypesHemangiosarcoma = cancerTypes.map(_.contains("11")),
      hsCancerTypesHistiocyticSarcoma = cancerTypes.map(_.contains("12")),
      hsCancerTypesHistiocytoma = cancerTypes.map(_.contains("13")),
      hsCancerTypesInsulinoma = cancerTypes.map(_.contains("14")),
      hsCancerTypesLeukemia = cancerTypes.map(_.contains("15")),
      hsCancerTypesLeiomyoma = cancerTypes.map(_.contains("16")),
      hsCancerTypesLeiomyosarcoma = cancerTypes.map(_.contains("17")),
      hsCancerTypesLipoma = cancerTypes.map(_.contains("18")),
      hsCancerTypesLymphomaLymphosarcoma = cancerTypes.map(_.contains("19")),
      hsCancerTypesMastCellTumor = cancerTypes.map(_.contains("20")),
      hsCancerTypesMelanoma = cancerTypes.map(_.contains("21")),
      hsCancerTypesMeningioma = cancerTypes.map(_.contains("22")),
      hsCancerTypesMultipleMyeloma = cancerTypes.map(_.contains("23")),
      hsCancerTypesOsteosarcoma = cancerTypes.map(_.contains("24")),
      hsCancerTypesPapilloma = cancerTypes.map(_.contains("25")),
      hsCancerTypesPeripheralNerveSheathTumor = cancerTypes.map(_.contains("26")),
      hsCancerTypesPlasmacytoma = cancerTypes.map(_.contains("27")),
      hsCancerTypesRhabdomyosarcoma = cancerTypes.map(_.contains("28")),
      hsCancerTypesSarcoma = cancerTypes.map(_.contains("29")),
      hsCancerTypesSebaceousAdenoma = cancerTypes.map(_.contains("30")),
      hsCancerTypesSoftTissueSarcoma = cancerTypes.map(_.contains("31")),
      hsCancerTypesSquamousCellCarcinoma = cancerTypes.map(_.contains("32")),
      hsCancerTypesThymoma = cancerTypes.map(_.contains("33")),
      hsCancerTypesTransitionalCellCarcinoma = cancerTypes.map(_.contains("34")),
      hsCancerTypesUnknown = cancerTypes.map(_.contains("99")),
      hsCancerTypesOther = cancerTypes.map(_.contains("98")),
      hsCancerTypesOtherDescription = if (cancerTypes.getOrElse(Array.empty).contains("98")) {
        rawRecord.getOptional("hs_dx_cancer_type_other")
      } else {
        None
      },
      hsLeukemiaTypesAcute = leukemiaTypes.map(_.contains("1")),
      hsLeukemiaTypesChronic = leukemiaTypes.map(_.contains("2")),
      hsLeukemiaTypesUnknown = leukemiaTypes.map(_.contains("99")),
      hsLeukemiaTypesOther = leukemiaTypes.map(_.contains("98")),
      hsLeukemiaTypesOtherDescription = if (leukemiaTypes.getOrElse(Array.empty).contains("98")) {
        rawRecord.getOptional("hs_dx_cancer_leuk_other")
      } else {
        None
      },
      hsLymphomaLymphosarcomaTypesBCell = lymphomaTypes.map(_.contains("1")),
      hsLymphomaLymphosarcomaTypesTCell = lymphomaTypes.map(_.contains("2")),
      hsLymphomaLymphosarcomaTypesTZone = lymphomaTypes.map(_.contains("3")),
      hsLymphomaLymphosarcomaTypesUnknown = lymphomaTypes.map(_.contains("99")),
      hsLymphomaLymphosarcomaTypesOther = lymphomaTypes.map(_.contains("98")),
      hsLymphomaLymphosarcomaTypesOtherDescription =
        if (lymphomaTypes.getOrElse(Array.empty).contains("98")) {
          rawRecord.getOptional("hs_dx_cancer_lymph_other")
        } else {
          None
        }
    )
  }
}
