package de.unikl.cs.dbis.waves.pipeline.split

import de.unikl.cs.dbis.waves.WavesSpec

import org.apache.spark.sql.functions.col

import de.unikl.cs.dbis.waves.partitions.visitors.operations._
import de.unikl.cs.dbis.waves.partitions.Bucket
import de.unikl.cs.dbis.waves.partitions.Spill
import de.unikl.cs.dbis.waves.pipeline._

class FlatShapeBuilderSpec extends WavesSpec {

  "The FlatShapeBuilder Step" when {
    "no buckets are given" should {
      "not be supported" in {
        (FlatShapeBuilder supports PipelineState(null, null)) shouldBe (false)
      }
    }
    "buckets are given" should {
      "be supported" in {
        (FlatShapeBuilder supports (Buckets(PipelineState(null, null)) = Seq())) shouldBe (true)
      }
      "derive the correct shape" when {
        "there are multiple buckets" in {
          Given("A state and a desired shape")
          val state = Buckets(PipelineState(null, null)) = Seq(null, null)

          When("we apply the FlatShapeBuilder step")
          val result = FlatShapeBuilder(state)

          Then("the correct shape is stored")
          (Shape isDefinedIn result) shouldBe (true)
          Shape(result) should equal (Spill(Bucket(()), Bucket(())))
        }
        "there is just one bucket" in {
          Given("A state and a desired shape")
          val state = Buckets(PipelineState(null, null)) = Seq(null)

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