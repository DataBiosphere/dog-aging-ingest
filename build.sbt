import org.broadinstitute.monster.sbt.model.JadeIdentifier

lazy val `dog-aging-ingest` = project
  .in(file("."))
  .enablePlugins(MonsterJadeDatasetPlugin)
  .settings(
    jadeDatasetName := JadeIdentifier
      .fromString("broad_dsp_dog_aging")
      .fold(sys.error, identity),
    jadeDatasetDescription := "Mirror of the Dog Aging Project, maintained by Broad's Data Sciences Platform",
    jadeTablePackage := "org.broadinstitute.monster.dogaging.jadeschema.table",
    jadeStructPackage := "org.broadinstitute.monster.dogaging.jadeschema.struct"
  )
