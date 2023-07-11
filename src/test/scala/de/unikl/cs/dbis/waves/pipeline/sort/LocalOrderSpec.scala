package de.unikl.cs.dbis.waves.pipeline.sort

import de.unikl.cs.dbis.waves.WavesSpec

import org.apache.spark.sql.functions.col

import de.unikl.cs.dbis.waves.partitions.visitors.operations._
import de.unikl.cs.dbis.waves.pipeline._

class LocalOrderSpec extends WavesSpec {

  "The LocalOrder Step" when {
    "the Column orderer is not supported and there are no Buckets " should {
      "not be supported" in {
        val state = PipelineState(null,null)
        val sorter = DummyColumnOrderer(false)
        (LocalOrder(sorter) supports state) shouldBe (false)
      }
    }
    "the Column orderer is not supported and there are Buckets " should {
      "not be supported" in {
        val state = Buckets(PipelineState(null,null)) = Seq()
        val sorter = DummyColumnOrderer(false)
        (LocalOrder(sorter) supports state) shouldBe (false)
      }
    }
    "the Column orderer is supported and there are no Buckets " should {
      "not be supported" in {
        val state = PipelineState(null,null)
        val sorter = DummyColumnOrderer(true)
        (LocalOrder(sorter) supports state) shouldBe (false)
      }
    }
    "the Column orderer is supported" should {
      "be supported" in {
        val state = Buckets(PipelineState(null,null)) = Seq()
        val sorter = DummyColumnOrderer(true)
        (LocalOrder(sorter) supports state) shouldBe (true)
      }
      "set the local sorter to the correct value" in {
        Given("A sorter and a state")
        val state = Buckets(PipelineState(null,null)) = Seq(null)
        val sorter = DummyColumnOrderer(true, Seq(col("a")))
        val step = LocalOrder(sorter)

        When("we apply the LocalOrder step")
        val result = step(state)

        Then("the correct order is stored")
        GlobalSortorder.isDefined(result) shouldBe (false)
        BucketSortorders.isDefined(result) shouldBe (true)
        val buckets = BucketSortorders(result)
        buckets should equal (Seq(Seq(col("a"))))
      }
    }
  }
}