package org.broadinstitute.monster.dap.common

import com.spotify.scio.testing.PipelineSpec
import org.apache.beam.sdk.options.{PipelineOptions, PipelineOptionsFactory}
import org.scalatest.matchers.should.Matchers

class TransformationHelperSpec extends PipelineSpec with Matchers {
  behavior of "TransformationHelper"

  it should "map JSON to a raw record grouped by dog ID and event name" in {
    val path = this.getClass.getResource("transformation_helper").toString
    val opts: PipelineOptions = PipelineOptionsFactory.fromArgs("--runner=DirectRunner").create()

    runWithRealContext(opts) { sc =>
      val records = TransformationHelper.readRecordsGroupByRecord(
        sc,
        path
      )

      records should haveSize(3)
      val ids = records.map { record =>
        record.id
      }

      val redcapEventNames = records.map { record =>
        record.getArray("redcap_event_name")
      }
      ids should containInAnyOrder(Seq(11111L, 22222L, 33333L))
      redcapEventNames should containInAnyOrder(
        Seq(
          Array("feb2020_arm_1", "fup_arm_1", "jan2020_arm_1"),
          Array("feb2020_arm_1", "jan2020_arm_1"),
          Array("feb2020_arm_1", "jan2020_arm_1")
        )
      )
    }
  }

}
