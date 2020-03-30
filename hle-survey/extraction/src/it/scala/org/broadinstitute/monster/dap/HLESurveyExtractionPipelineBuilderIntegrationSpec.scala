package org.broadinstitute.monster.dap

import java.time.{LocalDate, LocalTime, OffsetDateTime, ZoneOffset}

import better.files.File
import com.bettercloud.vault.{Vault, VaultConfig}
import org.broadinstitute.monster.common.PipelineBuilderSpec

class HLESurveyExtractionPipelineBuilderIntegrationSpec
    extends PipelineBuilderSpec[Args] {
  val outputDir = File.newTemporaryDirectory()
  override def afterAll(): Unit = outputDir.delete()

  val apiToken = {
    val baseConfig = new VaultConfig().address(sys.env("VAULT_ADDR"))

    val vaultConfig = baseConfig.token {
      val roleId = sys.env.get("VAULT_ROLE_ID")
      val secretId = sys.env.get("VAULT_SECRET_ID")

      roleId.zip(secretId).headOption match {
        case Some((roleId, secretId)) =>
          new Vault(baseConfig).auth().loginByAppRole(roleId, secretId).getAuthClientToken
        case None =>
          (File.home / ".vault-token").contentAsString.trim
      }
    }.build()

    // 1 here indicates we're still using v1 of the KV engine in Vault.
    new Vault(vaultConfig, 1)
      .logical()
      .read("secret/dsde/monster/dev/dog-aging/redcap-tokens/automation")
      .getData
      .get("token")
  }

  // We know that some records were updated in this time range, so it should
  // be fine to pull consistently without worrying about changes in data size.
  val start =
    OffsetDateTime.of(LocalDate.of(2020, 2, 1), LocalTime.MIDNIGHT, ZoneOffset.UTC)

  val end =
    OffsetDateTime.of(LocalDate.of(2020, 2, 2), LocalTime.MIDNIGHT, ZoneOffset.UTC)

  override val testArgs = Args(apiToken, Some(start), Some(end), outputDir.pathAsString)
  override val builder = HLESurveyExtractionPipeline.pipelineBuilder

  behavior of "HLESurveyExtractionPipelineBuilder"

  it should "successfully download some stuff from RedCap" in {
    readMsgs(outputDir) shouldNot be(empty)
  }
}
