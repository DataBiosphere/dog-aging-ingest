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
      val records = TransformationHelper.readRecordsGroupByEventName(
        sc,
        path
      )
      records should haveSize(6)
      val ids = records.map(record => record.id)
      ids should containInAnyOrder(Seq(11111L, 11111L, 22222L, 22222L, 33333L, 33333L))
    }
  }

  it should "map JSON to a raw record grouped by dog ID across multiple event names" in {
    val path = this.getClass.getResource("transformation_helper").toString
    val opts: PipelineOptions = PipelineOptionsFactory.fromArgs("--runner=DirectRunner").create()

    runWithRealContext(opts) { sc =>
      val records = TransformationHelper.readRecordsGroupByStudyId(
        sc,
        path
      )
      records should haveSize(3)
      val ids = records.map(record => record.id)
      ids should containInAnyOrder(Seq(11111L, 22222L, 33333L))
    }
  }
}
