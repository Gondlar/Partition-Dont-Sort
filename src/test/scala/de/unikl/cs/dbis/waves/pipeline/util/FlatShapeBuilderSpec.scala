package de.unikl.cs.dbis.waves.pipeline.util

import de.unikl.cs.dbis.waves.WavesSpec

import org.apache.spark.sql.functions.col

import de.unikl.cs.dbis.waves.partitions.visitors.operations._
import de.unikl.cs.dbis.waves.partitions.Bucket
import de.unikl.cs.dbis.waves.partitions.Spill
import de.unikl.cs.dbis.waves.pipeline._

class FlatShapeBuilderSpec extends WavesSpec with PipelineStateFixture {

  "The FlatShapeBuilder Step" when {
    "no bucket count is given" should {
      "not be supported" in {
        (FlatShapeBuilder supports dummyState) shouldBe (false)
      }
    }
    "the bucket count is given" should {
      "be supported" in {
        (FlatShapeBuilder supports (NumBuckets(dummyState) = 2)) shouldBe (true)
      }
      "derive the correct shape" when {
        "there are multiple buckets" in {
          Given("A state with multiple buckets")
          val state = NumBuckets(dummyState) = 2

          When("we apply the FlatShapeBuilder step")
          val result = FlatShapeBuilder(state)

          Then("the correct shape is stored")
          (Shape isDefinedIn result) shouldBe (true)
          Shape(result) should equal (Spill(Bucket(()), Bucket(())))
        }
        "there is just one bucket" in {
          Given("A state with one bucket")
          val state = NumBuckets(dummyState) = 1

          When("we apply the FlatShapeBuilder step")
          val result = FlatShapeBuilder(state)

          Then("the correct buckets are stored")
          (Shape isDefinedIn result) shouldBe (true)
          Shape(result) should equal (Bucket(()))
        }
      }
    }
  }
}