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
    val vaultConfig = new VaultConfig()
      .address(sys.env("VAULT_ADDR"))
      .token {
        sys.env.getOrElse(
          "VAULT_TOKEN",
          (File.home / ".vault-token").contentAsString.trim
        )
      }
      .build()
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