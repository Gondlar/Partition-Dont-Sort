package de.unikl.cs.dbis.waves.pipeline.split

import de.unikl.cs.dbis.waves.WavesSpec
import de.unikl.cs.dbis.waves.DataFrameFixture

import org.apache.spark.sql.functions.col

import de.unikl.cs.dbis.waves.partitions.visitors.operations._
import de.unikl.cs.dbis.waves.partitions.Bucket
import de.unikl.cs.dbis.waves.partitions.Spill
import de.unikl.cs.dbis.waves.pipeline._

class SingleBucketSpec extends WavesSpec
  with DataFrameFixture {

  "The FlatShapeBuilder Step" should {
    "always be supported" in {
      SingleBucket.isSupported(PipelineState(null, null)) shouldBe (true)
    }
    "set the buckets and shape correctly" in {
      Given("an empty state")
      val state = PipelineState(df, null)

      When("we apply the SingleBucket step")
      val result = SingleBucket(state)

      Then("the input data is the only bucket")
      Buckets.isDefined(result) shouldBe (true)
      val buckets = Buckets(result)
      buckets should have length (1)
      buckets(0) shouldBe theSameInstanceAs (df)

      And("the shape is a single bucket")
      Shape.isDefined(result) shouldBe (true)
      Shape(result) should equal (Bucket(()))
    }
  }
}