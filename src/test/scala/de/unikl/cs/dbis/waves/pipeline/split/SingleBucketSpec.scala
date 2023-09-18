package de.unikl.cs.dbis.waves.pipeline.split

import de.unikl.cs.dbis.waves.WavesSpec
import de.unikl.cs.dbis.waves.DataFrameFixture

import org.apache.spark.sql.functions.col

import de.unikl.cs.dbis.waves.partitions.visitors.operations._
import de.unikl.cs.dbis.waves.partitions.Bucket
import de.unikl.cs.dbis.waves.partitions.Spill
import de.unikl.cs.dbis.waves.pipeline._

class SingleBucketSpec extends WavesSpec
  with DataFrameFixture with PipelineStateFixture {

  "The FlatShapeBuilder Step" should {
    "always be supported" in {
      (SingleBucket supports dummyState) shouldBe (true)
    }
    "set the buckets and shape correctly" in {
      When("we apply the SingleBucket step")
      val result = SingleBucket(dummyDfState)

      Then("the input data is the only bucket")
      (Buckets isDefinedIn result) shouldBe (true)
      val buckets = Buckets(result)
      buckets should have length (1)
      buckets(0) shouldBe theSameInstanceAs (df)

      And("the shape is a single bucket")
      (Shape isDefinedIn result) shouldBe (true)
      Shape(result) should equal (Bucket(()))
    }
  }
}