package org.broadinstitute.monster.dap.afus

import org.broadinstitute.monster.dap.common.RawRecord
import org.broadinstitute.monster.dogaging.jadeschema.table.AfusCancerCondition

object AfusCancerTransformations {

  def mapCancerConditions(rawRecord: RawRecord): Option[AfusCancerCondition] = {
    if (rawRecord.getBoolean("fu_hs_dx_cancer_yn")) {
      val completeDate = rawRecord.getOptionalDate("fu_complete_date")
      val redcapEventName = rawRecord.getOptional("redcap_event_name")

      val cancerLocations = rawRecord.fields.get("fu_hs_dx_cancer_loc")

      val cancerTypes = rawRecord.fields.get("fu_hs_dx_cancer_type")

      val leukemiaTypes = rawRecord.fields.get("fu_hs_dx_cancer_leuk")

      val lymphomaTypes = rawRecord.fields.get("fu_hs_dx_cancer_lymph")

      Some(
        AfusCancerCondition(
          dogId = rawRecord.getRequired("study_id").toLong,
          afusCalendarYear = completeDate match {
            case Some(date) => Some(date.getYear.toLong)
            case None       => None
          },
          afusFollowupYear = redcapEventName match {
            case Some(event) =>
              if (event != "baseline_arm_1") Some(event.split("_")(1).toLong) else None
            case None => None
          },
          afusHsNewInitialDiagnosisYear = rawRecord.getOptionalNumber("fu_hs_dx_cancer_year"),
          afusHsNewInitialDiagnosisMonth = rawRecord.getOptionalNumber("fu_hs_dx_cancer_mo"),
          afusHsNewRequiredSurgeryOrHospitalization =
            rawRecord.getOptionalNumber("fu_hs_dx_cancer_surg"),
          afusHsNewFollowUpOngoing = rawRecord.getOptionalBoolean("fu_hs_dx_cancer_fu"),
          afusHsNewCancerLocationsAdrenalGland = cancerLocations.map(_.contains("1")),
          afusHsNewCancerLocationsAnalSac = cancerLocations.map(_.contains("2")),
          afusHsNewCancerLocationsBladderOrUrethra = cancerLocations.map(_.contains("3")),
          afusHsNewCancerLocationsBlood = cancerLocations.map(_.contains("4")),
          afusHsNewCancerLocationsBoneOrJoint = cancerLocations.map(_.contains("5")),
          afusHsNewCancerLocationsBrain = cancerLocations.map(_.contains("6")),
          afusHsNewCancerLocationsMammaryTissue = cancerLocations.map(_.contains("7")),
          afusHsNewCancerLocationsCardiacTissue = cancerLocations.map(_.contains("8")),
          afusHsNewCancerLocationsEar = cancerLocations.map(_.contains("9")),
          afusHsNewCancerLocationsEsophagus = cancerLocations.map(_.contains("10")),
          afusHsNewCancerLocationsEye = cancerLocations.map(_.contains("11")),
          afusHsNewCancerLocationsGallbladderOrBileDuct = cancerLocations.map(_.contains("12")),
          afusHsNewCancerLocationsGastrointestinalTract = cancerLocations.map(_.contains("13")),
          afusHsNewCancerLocationsKidney = cancerLocations.map(_.contains("14")),
          afusHsNewCancerLocationsLiver = cancerLocations.map(_.contains("15")),
          afusHsNewCancerLocationsLung = cancerLocations.map(_.contains("16")),
          afusHsNewCancerLocationsLymphNodes = cancerLocations.map(_.contains("17")),
          afusHsNewCancerLocationsMuscleOrSoftTissue = cancerLocations.map(_.contains("18")),
          afusHsNewCancerLocationsNoseOrNasalPassage = cancerLocations.map(_.contains("19")),
          afusHsNewCancerLocationsNerveSheath = cancerLocations.map(_.contains("20")),
          afusHsNewCancerLocationsOralCavity = cancerLocations.map(_.contains("21")),
          afusHsNewCancerLocationsOvaryOrUterus = cancerLocations.map(_.contains("22")),
          afusHsNewCancerLocationsPancreas = cancerLocations.map(_.contains("23")),
          afusHsNewCancerLocationsPerianalArea = cancerLocations.map(_.contains("24")),
          afusHsNewCancerLocationsPituitaryGland = cancerLocations.map(_.contains("25")),
          afusHsNewCancerLocationsProstate = cancerLocations.map(_.contains("26")),
          afusHsNewCancerLocationsRectum = cancerLocations.map(_.contains("27")),
          afusHsNewCancerLocationsSkinOfTrunkBodyHead = cancerLocations.map(_.contains("28")),
          afusHsNewCancerLocationsSkinOfLimbOrFoot = cancerLocations.map(_.contains("29")),
          afusHsNewCancerLocationsSpinalCord = cancerLocations.map(_.contains("30")),
          afusHsNewCancerLocationsSpleen = cancerLocations.map(_.contains("31")),
          afusHsNewCancerLocationsTesticle = cancerLocations.map(_.contains("32")),
          afusHsNewCancerLocationsThyroid = cancerLocations.map(_.contains("33")),
          afusHsNewCancerLocationsVenereal = cancerLocations.map(_.contains("34")),
          afusHsNewCancerLocationsUnknown = cancerLocations.map(_.contains("99")),
          afusHsNewCancerLocationsOther = cancerLocations.map(_.contains("98")),
          afusHsNewCancerLocationsOtherDescription =
            if (cancerLocations.getOrElse(Array.empty).contains("98")) {
              rawRecord.getOptionalStripped("fu_hs_dx_cancer_loc_other")
            } else {
              None
            },
          afusHsNewCancerTypesAdenoma = cancerTypes.map(_.contains("1")),
          afusHsNewCancerTypesAdenocarcinoma = cancerTypes.map(_.contains("2")),
          afusHsNewCancerTypesBasalCellTumor = cancerTypes.map(_.contains("3")),
          afusHsNewCancerTypesCarcinoma = cancerTypes.map(_.contains("4")),
          afusHsNewCancerTypesChondrosarcoma = cancerTypes.map(_.contains("5")),
          afusHsNewCancerTypesCystadenoma = cancerTypes.map(_.contains("6")),
          afusHsNewCancerTypesEpidermoidCyst = cancerTypes.map(_.contains("7")),
          afusHsNewCancerTypesEpulides = cancerTypes.map(_.contains("8")),
          afusHsNewCancerTypesFibrosarcoma = cancerTypes.map(_.contains("9")),
          afusHsNewCancerTypesHemangioma = cancerTypes.map(_.contains("10")),
          afusHsNewCancerTypesHemangiosarcoma = cancerTypes.map(_.contains("11")),
          afusHsNewCancerTypesHistiocyticSarcoma = cancerTypes.map(_.contains("12")),
          afusHsNewCancerTypesHistiocytoma = cancerTypes.map(_.contains("13")),
          afusHsNewCancerTypesInsulinoma = cancerTypes.map(_.contains("14")),
          afusHsNewCancerTypesLeukemia = cancerTypes.map(_.contains("15")),
          afusHsNewCancerTypesLeiomyoma = cancerTypes.map(_.contains("16")),
          afusHsNewCancerTypesLeiomyosarcoma = cancerTypes.map(_.contains("17")),
          afusHsNewCancerTypesLipoma = cancerTypes.map(_.contains("18")),
          afusHsNewCancerTypesLymphomaLymphosarcoma = cancerTypes.map(_.contains("19")),
          afusHsNewCancerTypesMastCellTumor = cancerTypes.map(_.contains("20")),
          afusHsNewCancerTypesMelanoma = cancerTypes.map(_.contains("21")),
          afusHsNewCancerTypesMeningioma = cancerTypes.map(_.contains("22")),
          afusHsNewCancerTypesMultipleMyeloma = cancerTypes.map(_.contains("23")),
          afusHsNewCancerTypesOsteosarcoma = cancerTypes.map(_.contains("24")),
          afusHsNewCancerTypesPapilloma = cancerTypes.map(_.contains("25")),
          afusHsNewCancerTypesPeripheralNerveSheathTumor = cancerTypes.map(_.contains("26")),
          afusHsNewCancerTypesPlasmacytoma = cancerTypes.map(_.contains("27")),
          afusHsNewCancerTypesRhabdomyosarcoma = cancerTypes.map(_.contains("28")),
          afusHsNewCancerTypesSarcoma = cancerTypes.map(_.contains("29")),
          afusHsNewCancerTypesSebaceousAdenoma = cancerTypes.map(_.contains("30")),
          afusHsNewCancerTypesSoftTissueSarcoma = cancerTypes.map(_.contains("31")),
          afusHsNewCancerTypesSquamousCellCarcinoma = cancerTypes.map(_.contains("32")),
          afusHsNewCancerTypesThymoma = cancerTypes.map(_.contains("33")),
          afusHsNewCancerTypesTransitionalCellCarcinoma = cancerTypes.map(_.contains("34")),
          afusHsNewCancerTypesUnknown = cancerTypes.map(_.contains("99")),
          afusHsNewCancerTypesOther = cancerTypes.map(_.contains("98")),
          afusHsNewCancerTypesOtherDescription =
            if (cancerTypes.getOrElse(Array.empty).contains("98")) {
              rawRecord.getOptionalStripped("fu_hs_dx_cancer_type_other")
            } else {
              None
            },
          afusHsNewLeukemiaTypesAcute = leukemiaTypes.map(_.contains("1")),
          afusHsNewLeukemiaTypesChronic = leukemiaTypes.map(_.contains("2")),
          afusHsNewLeukemiaTypesUnknown = leukemiaTypes.map(_.contains("99")),
          afusHsNewLeukemiaTypesOther = leukemiaTypes.map(_.contains("98")),
          afusHsNewLeukemiaTypesOtherDescription =
            if (leukemiaTypes.getOrElse(Array.empty).contains("98")) {
              rawRecord.getOptional("fu_hs_dx_cancer_leuk_other")
            } else {
              None
            },
          afusHsNewLymphomaLymphosarcomaTypesBCell = lymphomaTypes.map(_.contains("1")),
          afusHsNewLymphomaLymphosarcomaTypesTCell = lymphomaTypes.map(_.contains("2")),
          afusHsNewLymphomaLymphosarcomaTypesTZone = lymphomaTypes.map(_.contains("3")),
          afusHsNewLymphomaLymphosarcomaTypesUnknown = lymphomaTypes.map(_.contains("99")),
          afusHsNewLymphomaLymphosarcomaTypesOther = lymphomaTypes.map(_.contains("98")),
          afusHsNewLymphomaLymphosarcomaTypesOtherDescription =
            if (lymphomaTypes.getOrElse(Array.empty).contains("98")) {
              rawRecord.getOptional("fu_hs_dx_cancer_lymph_other")
            } else {
              None
            }
        )
      )
    } else {
      None
    }
  }
}
