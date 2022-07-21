package org.broadinstitute.monster.dap.afus

import com.spotify.scio.ScioContext
import com.spotify.scio.values.SCollection
import org.broadinstitute.monster.common.{PipelineBuilder, StorageIO}
import org.broadinstitute.monster.dap.common.{Args, RawRecord, TransformationHelper}
import org.slf4j.{Logger, LoggerFactory}

import java.time.LocalDate

object AfusTransformationPipelineBuilder extends PipelineBuilder[Args] {
  /**
    * Schedule all the steps for the Dog Aging transformation in the given pipeline context.
    *
    * Scheduled steps are launched against the context's runner when the `run()` method
    * is called on it.
    *
    * Adding implicit logger so we can associate it with the PipelineBuilder object
    */
  implicit val logger: Logger = LoggerFactory.getLogger(getClass)

  override def buildPipeline(ctx: ScioContext, args: Args): Unit = {
    val rawRecords: SCollection[RawRecord] = readRecords(ctx, args)

    //filter for form _completed flag
    //val filteredRecords = collectrawRecords.transform("Apply AFUS Filters")(_.flatMap(filterRecords))
    //val filteredRecords: SCollection[RawRecord] = rawRecords.transform("Apply AFUS Filters")(_.flatMap(filterRecords))
    val filteredRecords = rawRecords.filter(record => filterRecords(record))

    val owners =
      filteredRecords.transform("Map AFUS Owners")(
        _.flatMap(AfusOwnerTransformations.mapAfusOwnerData)
      )
    val dogs =
      filteredRecords.transform("Map AFUS Dogs")(_.flatMap(AfusDogTransformations.mapAfusDog))
    val healthConditions = filteredRecords.transform("Map AFUS Health Conditions")(
      _.flatMap(AfusHealthTransformations.mapHealthConditions)
    )
    val cancerConditions = filteredRecords.transform("Map AFUS Cancer Conditions")(
      _.flatMap(AfusCancerTransformations.mapCancerConditions)
    )

    StorageIO.writeJsonLists(owners, "AFUS Owners", s"${args.outputPrefix}/afus_owner")
    StorageIO.writeJsonLists(dogs, "AFUS Dogs", s"${args.outputPrefix}/afus_dog")
    StorageIO.writeJsonLists(
      healthConditions,
      "AFUS Health Conditions",
      s"${args.outputPrefix}/afus_health_condition"
    )
    StorageIO.writeJsonLists(
      cancerConditions,
      "AFUS Cancer Conditions",
      s"${args.outputPrefix}/afus_cancer_condition"
    )
    ()
  }

  // AFUS Filter Criteria
  // ([followup_status][fu_is_completed] = 1 AND [followup_status][fu_complete_date] <= 12/31/2021)
  // OR ([followup_status][fu_due_date] <= 12/31/2021 AND ([followup_owner_contact][[form]_complete] = 2 OR ...)
  def filterRecords(rawRecord: RawRecord): Boolean = {
    val afusDueDate = rawRecord.getOptionalDate("fu_due_date")
    val afusFullyComplete = rawRecord.getOptionalBoolean("fu_is_completed")
    val afusCompleteDate = rawRecord.getOptionalDate("fu_complete_date")
    (afusFullyComplete, afusCompleteDate) match {
      case (Some(fullyComplete), Some(completeDate)) =>
        fullyComplete && completeDate.isBefore(LocalDate.of(2022, 1, 1))
      case _ =>
        afusDueDate match {
          case Some(dueDate) =>
            dueDate.isBefore(LocalDate.of(2022, 1, 1)) &&
              (rawRecord.getOptionalNumber("followup_owner_contact_complete").contains(2)
                || rawRecord.getOptionalNumber("followup_owner_demographics_complete").contains(2)
                || rawRecord.getOptionalNumber("followup_dog_demographics_complete").contains(2)
                || rawRecord.getOptionalNumber("followup_physical_activity_complete").contains(2)
                || rawRecord.getOptionalNumber("followup_environment_complete").contains(2)
                || rawRecord.getOptionalNumber("followup_behavior_complete").contains(2)
                || rawRecord.getOptionalNumber("followup_diet_complete").contains(2)
                || rawRecord.getOptionalNumber("followup_meds_and_preventives_complete").contains(2)
                || rawRecord
                  .getOptionalNumber("followup_canine_eating_behavior_dora_complete")
                  .contains(2)
                || rawRecord
                  .getOptionalNumber("followup_dogowner_relationship_survey_mdors_complete")
                  .contains(2)
                || rawRecord.getOptionalNumber("followup_health_status_complete").contains(2))
          case _ =>
            false
        }
    }
//    afusDueDate match {
//      case Some(dueDate) =>
//        dueDate.isAfter(LocalDate.of(2021, 12, 31)) &&
//          afusOwnerContactComplete || afusFullyComplete || afusOwnerDemographicsComplete || afusDogDemographicsComplete || afusPhysicalActivityComplete || afusEnvironmentComplete || afusBehaviorComplete || afusDietComplete || afusMedsAndPreventativesComplete || afusDoraComplete || afusMdorsComplete || afusHealthStatusComplete
//      case _ =>
//        false
//    }
  }

  /** Read in records and group by study Id, with field name subgroups. */
  def readRecords(ctx: ScioContext, args: Args): SCollection[RawRecord] = {
    TransformationHelper.readRecordsGroupByStudyId(ctx, args.inputPrefix)
  }
}
