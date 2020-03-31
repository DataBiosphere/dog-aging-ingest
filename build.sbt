import org.broadinstitute.monster.sbt.model.JadeIdentifier

val okhttpVersion = "4.4.1"
val vaultDriverVersion = "5.1.0"

lazy val `dog-aging-ingest` = project
  .in(file("."))
  .aggregate(`dog-aging-hle-extraction`, `dog-aging-hle-transformation`)
  .settings(publish / skip := true)

lazy val `dog-aging-schema` = project
  .in(file("schema"))
  .enablePlugins(MonsterJadeDatasetPlugin)
  .settings(
    jadeDatasetName := JadeIdentifier
      .fromString("broad_dsp_dog_aging")
      .fold(sys.error, identity),
    jadeDatasetDescription := "Mirror of the Dog Aging Project, maintained by Broad's Data Sciences Platform",
    jadeTablePackage := "org.broadinstitute.monster.dogaging.jadeschema.table",
    jadeStructPackage := "org.broadinstitute.monster.dogaging.jadeschema.struct"
  )

lazy val `dog-aging-hle-extraction` = project
  .in(file("hle-survey/extraction"))
  .enablePlugins(MonsterScioPipelinePlugin)
  .settings(
    libraryDependencies += "com.squareup.okhttp3" % "okhttp" % okhttpVersion,
    libraryDependencies += "com.bettercloud" % "vault-java-driver" % vaultDriverVersion % IntegrationTest
  )

lazy val `dog-aging-hle-transformation` = project
  .in(file("hle-survey/transformation"))
  .enablePlugins(MonsterScioPipelinePlugin)
  .dependsOn(`dog-aging-schema`)
