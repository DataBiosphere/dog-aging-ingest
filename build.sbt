val okhttpVersion = "4.4.1"
val vaultDriverVersion = "5.1.0"

lazy val `dog-aging-ingest` = project
  .in(file("."))
  .aggregate(`dog-aging-schema`, `dog-aging-hle-extraction`, `dog-aging-hle-transformation`)
  .settings(publish / skip := true)

lazy val `dog-aging-schema` = project
  .in(file("schema"))
  .enablePlugins(MonsterJadeDatasetPlugin)
  .settings(
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
