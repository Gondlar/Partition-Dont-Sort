package de.unikl.cs.dbis.waves.pipeline.sink

import org.scalatest.Inspectors._
import de.unikl.cs.dbis.waves.WavesSpec

import de.unikl.cs.dbis.waves.pipeline._
import de.unikl.cs.dbis.waves.util.PartitionFolder

class PrioritySinkSpec extends WavesSpec with PipelineStateFixture {

  "A PrioritySink" when {
    "no Sink is supported" should {
      "not be supported" in {
        val sink = PrioritySink(makeDummies(false, false, false):_*)
        (sink supports dummyState) shouldBe (false)
      }
    }
    "one sink is supported" should {
      "be supported" in {
        val sink = PrioritySink(makeDummies(false, true, false):_*)
        (sink supports dummyState) shouldBe (true)
      }
      "pick the right sink" in {
        val sink = PrioritySink(makeDummies(false, true, false):_*)
        val (resultState, folders) = sink.run(dummyState)
        resultState.path should equal ("1")
        folders(0).name should equal ("1")
      }
    }
    "multiple sinks are supported" should {
      "be supported" in {
        val sink = PrioritySink(makeDummies(false, true, true):_*)
        (sink supports dummyState) shouldBe (true)
      }
      "pick the right sink" in {
        val sink = PrioritySink(makeDummies(false, true, true):_*)
        val (resultState, folders) = sink.run(dummyState)
        resultState.path should equal ("1")
        folders(0).name should equal ("1")
      }
    }
    "the chosen sink requires finalization should require finalization" in {
      val sink = PrioritySink(DummyPipelineSink(true, isFinalized = false))
      (sink isAlwaysFinalizedFor dummyState) shouldBe (false)
    }
    "the chosen sink does not require finalization should not require finalization" in {
      val sink = PrioritySink(DummyPipelineSink(true, isFinalized = true))
      (sink isAlwaysFinalizedFor dummyState) shouldBe (true)
    }
  }

  /**
    * Create a number of dummy sinks with the specified supported state.
    * As their result, they deliver their position in the list as the state's
    * path and the folder's name. This allows the tests to disabiguate them.
    */
  def makeDummies(supported: Boolean*)
    = for((isSupported, index) <- supported.zipWithIndex) yield {
      val folder = new PartitionFolder("", index.toString(), false)
      val result = PipelineState(null, index.toString())
      DummyPipelineSink(isSupported, Seq(folder), Some(result))
    }
}
