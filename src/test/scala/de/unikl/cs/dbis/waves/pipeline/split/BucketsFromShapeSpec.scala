package de.unikl.cs.dbis.waves.pipeline.split

import de.unikl.cs.dbis.waves.WavesSpec
import de.unikl.cs.dbis.waves.PartitionTreeFixture
import de.unikl.cs.dbis.waves.DataFrameFixture

import org.apache.spark.sql.functions.col

import de.unikl.cs.dbis.waves.partitions.visitors.operations._
import de.unikl.cs.dbis.waves.partitions.Bucket
import de.unikl.cs.dbis.waves.pipeline._

class BucketsFromShapeSpec extends WavesSpec
  with PartitionTreeFixture with DataFrameFixture {

  "The BucketsFromShape Step" when {
    "no shape is given" should {
      "not be supported" in {
        (BucketsFromShape supports PipelineState(null, null)) shouldBe (false)
      }
    }
    "a shape is given" should {
      "be supported" in {
        (BucketsFromShape supports (Shape(PipelineState(null, null)) = Bucket(()))) shouldBe (true)
      }
      "derive the correct buckets" when {
        "there are multiple buckets" in {
          Given("A state and a desired shape")
          val state = Shape(PipelineState(df, null)) = spill.shape

          When("we apply the BucketsFromShape step")
          val result = BucketsFromShape(state)

          Then("the correct buckets are stored")
          Buckets.isDefined(result) shouldBe (true)
          val buckets = Buckets(result)
          buckets.length should equal (3)
          buckets(0).collect shouldBe empty
          buckets(1).collect should contain theSameElementsAs (df.filter(col("b.d").isNull).collect)
          buckets(2).collect should contain theSameElementsAs (df.filter(col("b.d").isNotNull).collect)
        }
        "there is just one bucket" in {
          Given("A state and a desired shape")
          val state = Shape(PipelineState(df, null)) = bucket.shape

          When("we apply the BucketsFromShape step")
          val result = BucketsFromShape(state)

          Then("the correct buckets are stored")
          Buckets.isDefined(result) shouldBe (true)
          val buckets = Buckets(result)
          buckets.length should equal (1)
          buckets(0).collect should contain theSameElementsAs (df.collect)
        }
      }
    }
  }
}