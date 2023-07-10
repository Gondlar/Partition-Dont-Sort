package de.unikl.cs.dbis.waves.pipeline.sort

import de.unikl.cs.dbis.waves.WavesSpec

import org.apache.spark.sql.functions.col

import de.unikl.cs.dbis.waves.partitions.visitors.operations._
import de.unikl.cs.dbis.waves.pipeline._

class GlobalOrderSpec extends WavesSpec {

  "The GlobalOrder Step" when {
    "the Column orderer is not supported" should {
      "not be supported" in {
        val state = PipelineState(null,null)
        val sorter = DummyColumnOrderer(false)
        GlobalOrder(sorter).isSupported(state) shouldBe (false)
      }
    }
    "the Column orderer is supported" should {
      "be supported" in {
        val state = PipelineState(null,null)
        val sorter = DummyColumnOrderer(true)
        GlobalOrder(sorter).isSupported(state) shouldBe (true)
      }
      "set the global sorter to the correct value" in {
        Given("A sorter and a state")
        val state = PipelineState(null, null)
        val sorter = DummyColumnOrderer(true, Seq(col("a")))
        val step = GlobalOrder(sorter)

        When("we apply the GlobalOrder step")
        val result = step(state)

        Then("the correct order is stored")
        GlobalSortorder(result) should equal (Seq(col("a")))
        BucketSortorders.isDefined(result) shouldBe (false)
      }
    }
  }
}