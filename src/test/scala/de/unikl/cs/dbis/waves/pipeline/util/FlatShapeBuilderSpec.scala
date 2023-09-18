package de.unikl.cs.dbis.waves.pipeline.util

import de.unikl.cs.dbis.waves.WavesSpec

import org.apache.spark.sql.functions.col

import de.unikl.cs.dbis.waves.partitions.visitors.operations._
import de.unikl.cs.dbis.waves.partitions.Bucket
import de.unikl.cs.dbis.waves.partitions.Spill
import de.unikl.cs.dbis.waves.pipeline._

class FlatShapeBuilderSpec extends WavesSpec with PipelineStateFixture {

  "The FlatShapeBuilder Step" when {
    "no buckets are given" should {
      "not be supported" in {
        (FlatShapeBuilder supports dummyState) shouldBe (false)
      }
    }
    "buckets are given" should {
      "be supported" in {
        (FlatShapeBuilder supports (Buckets(dummyState) = Seq())) shouldBe (true)
      }
      "derive the correct shape" when {
        "there are multiple buckets" in {
          Given("A state with multiple buckets")
          val state = Buckets(dummyState) = Seq(null, null)

          When("we apply the FlatShapeBuilder step")
          val result = FlatShapeBuilder(state)

          Then("the correct shape is stored")
          (Shape isDefinedIn result) shouldBe (true)
          Shape(result) should equal (Spill(Bucket(()), Bucket(())))
        }
        "there is just one bucket" in {
          Given("A state with one bucket")
          val state = Buckets(dummyState) = Seq(null)

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