import _root_.io.circe.Json

val enumeratumVersion = "1.6.1"
val okhttpVersion = "4.4.1"
val vaultDriverVersion = "5.1.0"

lazy val `dog-aging-ingest` = project
  .in(file("."))
  .aggregate(`dog-aging-schema`, `dog-aging-hles-extraction`, `dog-aging-hles-transformation`)
  .settings(publish / skip := true)

lazy val `dog-aging-schema` = project
  .in(file("schema"))
  .enablePlugins(MonsterJadeDatasetPlugin)
  .settings(
    jadeTablePackage := "org.broadinstitute.monster.dogaging.jadeschema.table",
    jadeTableFragmentPackage := "org.broadinstitute.monster.dogaging.jadeschema.fragment",
    jadeStructPackage := "org.broadinstitute.monster.dogaging.jadeschema.struct"
  )

lazy val `dog-aging-hles-extraction` = project
  .in(file("dap-etl/extraction"))
  .enablePlugins(MonsterScioPipelinePlugin)
  .settings(
    libraryDependencies += "com.squareup.okhttp3" % "okhttp" % okhttpVersion,
    libraryDependencies += "com.squareup.okhttp3" % "logging-interceptor" % okhttpVersion,
    libraryDependencies += "com.squareup.okhttp3" % "mockwebserver" % okhttpVersion % Test,
    libraryDependencies += "com.bettercloud" % "vault-java-driver" % vaultDriverVersion % IntegrationTest,
    IntegrationTest / parallelExecution := false
  )

lazy val `dog-aging-hles-transformation` = project
  .in(file("dap-etl/transformation"))
  .enablePlugins(MonsterScioPipelinePlugin)
  .dependsOn(`dog-aging-schema`)
  .settings(
    libraryDependencies += "com.beachape" %% "enumeratum" % enumeratumVersion
  )
