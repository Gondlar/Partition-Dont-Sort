package de.unikl.cs.dbis.waves.pipeline.util

import org.scalatest.Inspectors._
import de.unikl.cs.dbis.waves.WavesSpec

import de.unikl.cs.dbis.waves.pipeline._
import de.unikl.cs.dbis.waves.util.PartitionFolder

class PriorityStepSpec extends WavesSpec with PipelineStateFixture {

  "A PriorityStep" when {
    "no Step is supported" should {
      "not be supported" in {
        val sink = PriorityStep(makeDummies(false, false, false):_*)
        (sink supports dummyState) shouldBe (false)
      }
    }
    "one sink is supported" should {
      "be supported" in {
        val sink = PriorityStep(makeDummies(false, true, false):_*)
        (sink supports dummyState) shouldBe (true)
      }
      "pick the right sink" in {
        val sink = PriorityStep(makeDummies(false, true, false):_*)
        val result = sink.run(dummyState)
        result.path should equal ("1")
      }
    }
    "multiple sinks are supported" should {
      "be supported" in {
        val sink = PriorityStep(makeDummies(false, true, true):_*)
        (sink supports dummyState) shouldBe (true)
      }
      "pick the right sink" in {
        val sink = PriorityStep(makeDummies(false, true, true):_*)
        val result = sink.run(dummyState)
        result.path should equal ("1")
      }
    }
  }

  /**
    * Create a number of dummy steps with the specified supported state.
    * As their result, they deliver their position in the list as the state's
    * path. This allows the tests to disabiguate them.
    */
  def makeDummies(supported: Boolean*)
    = for((isSupported, index) <- supported.zipWithIndex)
      yield DummyStep(isSupported, index.toString())
}

final case class DummyStep(
  supported: Boolean,
  id: String
) extends PipelineStep {

  override def supports(state: PipelineState): Boolean
    = supported

  override def run(state: PipelineState): PipelineState
    = new PipelineState(null, id)
}
