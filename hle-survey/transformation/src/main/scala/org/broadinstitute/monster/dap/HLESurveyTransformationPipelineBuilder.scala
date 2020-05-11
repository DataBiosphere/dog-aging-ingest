package org.broadinstitute.monster.dap

import com.spotify.scio.ScioContext
import com.spotify.scio.coders.Coder
import com.spotify.scio.values.SCollection
import org.broadinstitute.monster.common.{PipelineBuilder, StorageIO}
import org.broadinstitute.monster.common.msg._
import org.broadinstitute.monster.dogaging.jadeschema.table.{HlesDog, HlesOwner}
import upack.Msg

object HLESurveyTransformationPipelineBuilder extends PipelineBuilder[Args] {

  implicit val msgCoder: Coder[Msg] = Coder.beam(new UpackMsgCoder)

  case class RawRecord(id: Long, fields: Map[String, Array[String]]) {

    def getRequired(field: String): String = fields(field).head

    def getOptional(field: String): Option[String] = fields.get(field).flatMap(_.headOption)

    def getArray(field: String): Array[String] = fields.get(field).getOrElse(Array.empty)
  }

  /**
    * Schedule all the steps for the Dog Aging transformation in the given pipeline context.
    *
    * Scheduled steps are launched against the context's runner when the `run()` method
    * is called on it.
    */
  override def buildPipeline(ctx: ScioContext, args: Args): Unit = {
    val rawRecords = readRecords(ctx, args)
    val dogs = rawRecords.transform("Map Dogs")(_.map(mapDog))
    val owners = rawRecords.transform("Map Owners")(_.map(mapOwner))

    StorageIO.writeJsonLists(dogs, "Dogs", s"${args.outputPrefix}/hles_dog")
    StorageIO.writeJsonLists(owners, "Owners", s"${args.outputPrefix}/hles_owner")
    ()
  }

  /**
    * Read in records and group by study Id, with field name subgroups.
    * Output the format: (studyId, Iterable[(fieldName, Iterable[value])])
    */
  def readRecords(ctx: ScioContext, args: Args): SCollection[RawRecord] = {
    val rawRecords = StorageIO
      .readJsonLists(
        ctx,
        "Raw Records",
        s"${args.inputPrefix}/records/*.json"
      )

    // Group by study ID (record number) and field name
    // to get the format: (studyId, Iterable((fieldName, Iterable(value))))
    rawRecords
      .groupBy(_.read[String]("record"))
      .map {
        case (id, rawRecordValues) =>
          val fields = rawRecordValues
            .groupBy(_.read[String]("field_name"))
            .map {
              case (fieldName, rawValues) =>
                (fieldName, rawValues.map(_.read[String]("value")).toArray)
            }
          RawRecord(id.toLong, fields)
      }
  }

  def mapOwner(rawRecord: RawRecord): HlesOwner = HlesOwner(
    // FIXME: Once DAP figures out a name for a dedicated owner ID, use that.
    ownerId = rawRecord.id,
    odAgeRangeYears = None,
    odMaxEducation = None,
    odMaxEducationOther = None,
    odRace = Array.empty,
    odRaceOther = None,
    odHispanic = None,
    odAnnualIncomeRangeUsd = None,
    ocHouseholdAdultCount = None,
    ocHouseholdChildCount = None,
    ssHouseholdDogCount = None,
    ocPrimaryResidenceState = None,
    ocPrimaryResidenceCensusDivision = None,
    ocPrimaryResidenceZip = None,
    ocPrimaryResidenceOwnership = None,
    ocPrimaryResidenceOwnershipOther = None,
    ocSecondaryResidenceState = None,
    ocSecondaryResidenceZip = None,
    ocSecondaryResidenceOwnership = None,
    ocSecondaryResidenceOwnershipOther = None
  )

  def mapDog(rawRecord: RawRecord): HlesDog =
    HlesDog(
      dogId = rawRecord.id,
      // FIXME: Once DAP figures out a name for a dedicated owner ID, use that.
      ownerId = rawRecord.id,
      ddBreedPure = None,
      ddBreedPureNonAkc = None,
      ddBreedMixedPrimary = None,
      ddBreedMixedSecondary = None,
      ddAgeYears = None,
      ddAgeBasis = None,
      ddAgeExactSource = None,
      ddAgeExactSourceOther = None,
      ddAgeEstimateSource = None,
      ddAgeEstimateSourceOther = None,
      ddBirthYear = None,
      ddBirthMonth = None,
      ddSex = None,
      ddSpayedOrNeutered = None,
      ddSpayOrNeuterAge = None,
      ddSpayMethod = None,
      ddEstrousCycleExperiencedBeforeSpayed = None,
      ddEstrousCycleCount = None,
      ddHasBeenPregnant = None,
      ddHasSiredLitters = None,
      ddLitterCount = None,
      ddWeightRange = None,
      ddWeightLbs = None,
      ddWeightRangeExpectedAdult = None,
      ddInsuranceProvider = None,
      ddInsuranceProviderOther = None,
      ddAcquiredYear = None,
      ddAcquiredMonth = None,
      ddAcquiredSource = None,
      ddAcquiredSourceOther = None,
      ddAcquiredCountry = None,
      ddAcquiredState = None,
      ddAcquiredZip = None,
      ddPrimaryRole = None,
      ddPrimaryRoleOther = None,
      ddSecondaryRole = None,
      ddSecondaryRoleOther = None,
      ddOtherRoles = Array.empty,
      ddServiceTypes = Array.empty,
      ddServiceTypesOtherMedical = None,
      ddServiceTypesOtherHealth = None,
      ddServiceTypesOther = None,
      ocPrimaryResidenceState = None,
      ocPrimaryResidenceCensusDivision = None,
      ocPrimaryResidenceZip = None,
      ocPrimaryResidenceOwnership = None,
      ocPrimaryResidenceOwnershipOther = None,
      ocPrimaryResidenceTimePercentage = None,
      ocSecondaryResidenceState = None,
      ocSecondaryResidenceZip = None,
      ocSecondaryResidenceOwnership = None,
      ocSecondaryResidenceOwnershipOther = None,
      ocSecondaryResidenceTimePercentage = None
    )
}
