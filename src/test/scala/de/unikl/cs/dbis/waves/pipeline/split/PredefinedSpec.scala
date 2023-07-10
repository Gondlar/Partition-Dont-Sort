package de.unikl.cs.dbis.waves.pipeline.split

import de.unikl.cs.dbis.waves.WavesSpec
import de.unikl.cs.dbis.waves.PartitionTreeFixture

import de.unikl.cs.dbis.waves.partitions.visitors.operations._
import de.unikl.cs.dbis.waves.pipeline.PipelineState
import de.unikl.cs.dbis.waves.pipeline.Shape

class PredefinedSpec extends WavesSpec
  with PartitionTreeFixture {

  "The Predefined Step" should {
    "always be supported" in {
      val step = Predefined(null)
      val state = PipelineState(null, null)
      step.isSupported(state) shouldBe (true)
    }
    "set the shape to the predefined value" in {
      Given("A state and a desired shape")
      val state = Shape(PipelineState(null, null)) = spill.shape
      val step = Predefined(split.shape)

      When("we apply the Predefined step")
      val result = step(state)

      Then("the correct shape is stored")
      Shape(result) should equal (split.shape)
    }
  }
}