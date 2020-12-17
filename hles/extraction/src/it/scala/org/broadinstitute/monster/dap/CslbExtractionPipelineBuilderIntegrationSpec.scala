package org.broadinstitute.monster.dap

import java.time.{LocalDate, LocalTime, OffsetDateTime, ZoneOffset}

import better.files.File
import com.bettercloud.vault.{SslConfig, Vault, VaultConfig}
import org.broadinstitute.monster.common.PipelineBuilderSpec

class CslbExtractionPipelineBuilderIntegrationSpec extends PipelineBuilderSpec[Args] {
  import org.broadinstitute.monster.common.msg.MsgOps

  val outputDir = File.newTemporaryDirectory()
  val cslbOutputDir = File(outputDir, CslbExtractionPipeline.subdir)
  override def afterAll(): Unit = outputDir.delete()

  val apiToken = {
    val baseConfig = new VaultConfig()
      .address(sys.env("VAULT_ADDR"))
      .sslConfig(new SslConfig().verify(false))

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
    OffsetDateTime.of(LocalDate.of(2020, 11, 15), LocalTime.MIDNIGHT, ZoneOffset.UTC)

  val end =
    OffsetDateTime.of(LocalDate.of(2020, 11, 16), LocalTime.MIDNIGHT, ZoneOffset.UTC)

  override val testArgs =
    Args(apiToken, Some(start), Some(end), outputDir.pathAsString, pullDataDictionaries = false)
  override val builder = CslbExtractionPipeline.pipelineBuilder

  behavior of "CslbSurveyExtractionPipelineBuilder"

  it should "successfully download records from RedCap" in {
    readMsgs(cslbOutputDir, "records/*.json") shouldNot be(empty)
  }

  it should "only download records that have completed all CSLB instruments" in {
    readMsgs(cslbOutputDir, "records/*.json").foreach { record =>
      CslbExtractionPipeline.extractionFilters
        .find(directive => directive.field == record.read[String]("field_name"))
        .foreach(expected => record.read[String]("value") shouldBe expected.field)
    }
  }
}