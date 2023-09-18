package de.unikl.cs.dbis.waves.pipeline.sink

import org.scalatest.Inspectors._
import de.unikl.cs.dbis.waves.WavesSpec
import de.unikl.cs.dbis.waves.PartitionTreeFixture
import de.unikl.cs.dbis.waves.DataFrameFixture
import de.unikl.cs.dbis.waves.TempFolderFixture

import org.apache.spark.sql.functions.col

import de.unikl.cs.dbis.waves.pipeline._
import de.unikl.cs.dbis.waves.partitions.Bucket
import de.unikl.cs.dbis.waves.partitions.SplitByPresence
import de.unikl.cs.dbis.waves.partitions.visitors.operations._

class ParallelSinkSpec extends WavesSpec
  with DataFrameFixture with TempFolderFixture with PartitionTreeFixture with PipelineStateFixture {

  "A ParallelSink" when {
    "shape are undefined" should {
      "not be supported" in {
        (ParallelSink supports dummyState) shouldBe (false)
      }
    }
    "schema modifications are requested" should {
      "not be supported" in {
        val withShape = Shape(dummyState) = Bucket(())
        val withSchemaModifications = ModifySchema(withShape) = true
        (ParallelSink supports withSchemaModifications) shouldBe (false)
      }
    }
    "shape is defined" should {
      "be supported" in {
        val withShape = Shape(dummyState) = Bucket(())
        (ParallelSink supports withShape) shouldBe (true)
      }
      "not require finaliration" in {
        (ParallelSink isAlwaysFinalizedFor dummyState) shouldBe (true)
      }
      "store each bucket as a Partition" when {
        "there are multiple buckets" in {
          Given("A PipelineState with multiple buckets")
          val emptyState = PipelineState(df, tempDirectory)
          val withBuckets = Shape(emptyState) = split.shape

          When("we run the ParallelSink")
          val (finalState, result) = ParallelSink.run(withBuckets)

          Then("the written partitions look as expected")
          result should have length (2)
          spark.read.parquet(result(0).filename).collect() should contain theSameElementsInOrderAs (df.filter(col("b.d").isNull).collect())
          spark.read.parquet(result(1).filename).collect() should contain theSameElementsInOrderAs (df.filter(col("b.d").isNotNull).collect())

          And("the final state is unchanged")
          finalState should equal (withBuckets)
        }
        "there is only one bucket" in {
          Given("A PipelineState with one Bucket")
          val emptyState = PipelineState(df, tempDirectory)
          val withBuckets = Shape(emptyState) = Bucket(())

          When("we run the ParallelSink")
          val (finalState, result) = ParallelSink.run(withBuckets)

          Then("the written partitions look as expected")
          result should have length (1)
          spark.read.parquet(result(0).filename).collect() should contain theSameElementsInOrderAs (df.collect())

          And("the final state is unchanged")
          finalState should equal (withBuckets)
        }
        "there is an empty bucket" in {
          Given("A PipelineState with an empty bucket")
          val emptyState = PipelineState(df, tempDirectory)
          val withBuckets = Shape(emptyState) = SplitByPresence("e", (), ())

          When("we run the ParallelSink")
          val (finalState, result) = ParallelSink.run(withBuckets)

          Then("the written partitions look as expected")
          result should have length (2)
          spark.read.parquet(result(1).filename).collect() should contain theSameElementsInOrderAs (df.collect())

          And("the final state is unchanged")
          finalState should equal (withBuckets)
        }
      }
    }
  }
}
