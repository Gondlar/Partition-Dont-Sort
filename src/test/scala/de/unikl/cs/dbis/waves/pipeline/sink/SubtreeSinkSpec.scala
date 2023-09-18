package de.unikl.cs.dbis.waves.pipeline.sink

import org.scalatest.Inspectors._
import de.unikl.cs.dbis.waves.WavesSpec
import de.unikl.cs.dbis.waves.PartitionTreeFixture
import de.unikl.cs.dbis.waves.TempFolderFixture

import de.unikl.cs.dbis.waves.pipeline._
import de.unikl.cs.dbis.waves.partitions.visitors.operations._
import de.unikl.cs.dbis.waves.util.PartitionFolder
import de.unikl.cs.dbis.waves.partitions.{PartitionTreePath, Partitioned, Rest, Present, Absent}
import de.unikl.cs.dbis.waves.partitions.TreeNode.AnyNode

class SubtreeSinkSpec extends WavesSpec
  with PartitionTreeFixture with TempFolderFixture {

  "A SubtreeSink" should {
    "not be constructable from an invalid path" in {
      an [IllegalArgumentException] shouldBe thrownBy (SubtreeSink(null, split, Seq(Rest)))
    }
    "not be supported" when {
      "the delegate is unsupported" in {
        val state = Shape(PipelineState(null, null)) = split.shape
        (SubtreeSink(DummyPipelineSink(false), split, Seq.empty) supports state) shouldBe (false)
      }
      "the shape is undefined" in {
        val state = PipelineState(null, null)
        (SubtreeSink(DummyPipelineSink(true), split, Seq.empty) supports state) shouldBe (false)
      }
      "both shape is undefined and the delegate is unsupported" in {
        val state = PipelineState(null, null)
        (SubtreeSink(DummyPipelineSink(false), split, Seq.empty) supports state) shouldBe (false)
      }
    }
    "be supported" when {
      "both the shape is defined and the delegate is suppported" in {
        val state = Shape(PipelineState(null, null)) = split.shape
        (SubtreeSink(DummyPipelineSink(true), split, Seq.empty) supports state) shouldBe (true)
      }
    }
    "require finalization" when {
      "the delegate requires finalization" in {
        val state = PipelineState(null, null)
        val delgate = DummyPipelineSink(true, isFinalized = false)
        val sink = SubtreeSink(delgate, split, Seq.empty)
        (sink isAlwaysFinalizedFor state) shouldBe (false)
      }
      "the delegate does not require finalization" in {
        val state = PipelineState(null, null)
        val delgate = DummyPipelineSink(true, isFinalized = true)
        val sink = SubtreeSink(delgate, split, Seq.empty)
        (sink isAlwaysFinalizedFor state) shouldBe (true)
      }
    }
    "write the correct subtree" when {
      "we replace a Present node" in replacement (
        Seq(Partitioned, Present),
        Seq( new PartitionFolder(tempDirectory, "foo3", false)
           , new PartitionFolder(tempDirectory, "baz2", false)
           , new PartitionFolder(tempDirectory, "dummy1", false)
           , new PartitionFolder(tempDirectory, "dummy2", false)
           )
      )
      "we replace an Absent node" in replacement (
        Seq(Partitioned, Absent),
        Seq( new PartitionFolder(tempDirectory, "foo3", false)
           , new PartitionFolder(tempDirectory, "dummy1", false)
           , new PartitionFolder(tempDirectory, "dummy2", false)
           , new PartitionFolder(tempDirectory, "bar2", false)
           )
      )
      "we replace a Partitioned node" in replacement(
        Seq(Partitioned),
        Seq( new PartitionFolder(tempDirectory, "foo3", false)
           , new PartitionFolder(tempDirectory, "dummy1", false)
           , new PartitionFolder(tempDirectory, "dummy2", false)
           )
      )
      "we replace a Rest node" in replacement(
        Seq(Rest),
        Seq( new PartitionFolder(tempDirectory, "dummy", false)
           , new PartitionFolder(tempDirectory, "baz2", false)
           , new PartitionFolder(tempDirectory, "bar2", false)
           ),
        bucket.shape,
        Seq(new PartitionFolder(tempDirectory, "dummy", false))
      )
      "we replace everything" in replacement (
        Seq(),
        Seq( new PartitionFolder(tempDirectory, "dummy1", false)
           , new PartitionFolder(tempDirectory, "dummy2", false)
           )
      )
    }
  }

  def replacement(
    substituePath: Seq[PartitionTreePath],
    expectedResult: => Seq[PartitionFolder],
    newShape: AnyNode[_] = split,
    dummyBuckets: => Seq[PartitionFolder] = Seq(
        new PartitionFolder(tempDirectory, "dummy1", false),
        new PartitionFolder(tempDirectory, "dummy2", false)
      )
  ) = {
    Given("a shape and a supported delegate")
      val state = Shape(PipelineState(null, tempDirectory)) = newShape.shape
      val dummyDelegate = DummyPipelineSink(true, dummyBuckets)
      val sink = SubtreeSink(dummyDelegate, spill, substituePath)

      When("we run the sink")
      val (resultState, result) = sink.run(state)

      Then("the folders are correct")
      result should contain theSameElementsInOrderAs (expectedResult)

      And("the shape is correct")
      Shape(resultState) should equal (spill.shape.replace(substituePath, newShape.shape))
  }
}
