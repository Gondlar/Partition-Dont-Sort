package de.unikl.cs.dbis.waves.pipeline.split

import de.unikl.cs.dbis.waves.WavesSpec
import de.unikl.cs.dbis.waves.PartitionTreeFixture

import de.unikl.cs.dbis.waves.partitions.visitors.operations._
import de.unikl.cs.dbis.waves.pipeline.PipelineState
import de.unikl.cs.dbis.waves.pipeline.Shape
import de.unikl.cs.dbis.waves.pipeline.PipelineStateFixture

class PredefinedSpec extends WavesSpec
  with PartitionTreeFixture with PipelineStateFixture {

  "The Predefined Step" should {
    "always be supported" in {
      val step = Predefined(null)
      (step supports dummyState) shouldBe (true)
    }
    "set the shape to the predefined value" in {
      Given("A state and a desired shape")
      val state = Shape(dummyState) = spill.shape
      val step = Predefined(split.shape)

      When("we apply the Predefined step")
      val result = step(state)

      Then("the correct shape is stored")
      Shape(result) should equal (split.shape)
    }
  }
}