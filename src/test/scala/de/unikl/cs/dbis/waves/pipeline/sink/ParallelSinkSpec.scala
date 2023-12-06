package de.unikl.cs.dbis.waves.pipeline.sink

import org.scalatest.Inspectors._
import de.unikl.cs.dbis.waves.WavesSpec
import de.unikl.cs.dbis.waves.PartitionTreeFixture
import de.unikl.cs.dbis.waves.DataFrameFixture
import de.unikl.cs.dbis.waves.TempFolderFixture

import org.apache.spark.sql.functions.{col,when,typedLit}

import de.unikl.cs.dbis.waves.pipeline._

class ParallelSinkSpec extends WavesSpec
  with DataFrameFixture with TempFolderFixture with PartitionTreeFixture with PipelineStateFixture {

  "A ParallelSink" when {
    "shape are undefined" should {
      "not be supported" in {
        (ParallelSink supports dummyState) shouldBe (false)
      }
    }
    "the number of Buckets is defined" should {
      "not be supported" in {
        val withBucketCount = NumBuckets(dummyState) = 1
        (ParallelSink supports withBucketCount) shouldBe (false)
      }
    }
    "schema modifications are requested" should {
      "not be supported" in {
        val withBucketCount = NumBuckets(dummyState) = 1
        val withShuffleColumn = ShuffleColumn(withBucketCount) = "shuffle"
        val withSchemaModifications = ModifySchema(withShuffleColumn) = true
        (ParallelSink supports withSchemaModifications) shouldBe (false)
      }
    }
    "the shuffle column is defined" should {
      "be supported" in {
        val withBucketCount = NumBuckets(dummyState) = 1
        val withShuffleColumn = ShuffleColumn(withBucketCount) = "shuffle"
        (ParallelSink supports withShuffleColumn) shouldBe (true)
      }
      "not require finalization" in {
        (ParallelSink isAlwaysFinalizedFor dummyState) shouldBe (true)
      }
      "store each bucket as a Partition" when {
        "there are multiple buckets" in {
          Given("A PipelineState with multiple buckets")
          val shuffleColumnName = "shuffle"
          val numBuckets = 2

          val shuffledDf = df
            .withColumn(shuffleColumnName, when(col("b.d").isNull, 0).otherwise(1))
            .repartition(numBuckets, col(shuffleColumnName))
          val emptyState = PipelineState(shuffledDf, tempDirectory)
          val withNumBuckets = NumBuckets(emptyState) = numBuckets
          val withShuffleColumn = ShuffleColumn(withNumBuckets) = shuffleColumnName

          When("we run the ParallelSink")
          val (finalState, result) = ParallelSink.run(withShuffleColumn)

          Then("the written partitions look as expected")
          result should have length (numBuckets)
          spark.read.parquet(result(0).filename).collect() should contain theSameElementsInOrderAs (df.filter(col("b.d").isNull).collect())
          spark.read.parquet(result(1).filename).collect() should contain theSameElementsInOrderAs (df.filter(col("b.d").isNotNull).collect())

          And("the final state is unchanged, but the shuffle column is gone")
          Schema(finalState) should equal (schema)
          finalState.copy(data = df) should equal (withShuffleColumn.copy(data = df))
        }
        "there is only one bucket" in {
          Given("A PipelineState with one Bucket")
          val shuffleColumnName = "shuffle"
          val numBuckets = 1

          val shuffledDf = df
            .withColumn(shuffleColumnName, typedLit(0))
            .repartition(1)
          val emptyState = PipelineState(shuffledDf, tempDirectory)
          val withNumBuckets = NumBuckets(emptyState) = numBuckets
          val withShuffleColumn = ShuffleColumn(withNumBuckets) = shuffleColumnName

          When("we run the ParallelSink")
          val (finalState, result) = ParallelSink.run(withShuffleColumn)

          Then("the written partitions look as expected")
          result should have length (1)
          spark.read.parquet(result(0).filename).collect() should contain theSameElementsInOrderAs (df.collect())

          And("the final state is unchanged, but the shuffle column is gone")
          Schema(finalState) should equal (schema)
          finalState.copy(data = df) should equal (withShuffleColumn.copy(data = df))
        }
        "there is an empty bucket" in {
          Given("A PipelineState with an empty bucket")
          val shuffleColumnName = "shuffle"
          val numBuckets = 2

          val shuffledDf = df
            .withColumn(shuffleColumnName, typedLit(1))
            .repartition(2, col(shuffleColumnName))
          val emptyState = PipelineState(shuffledDf, tempDirectory)
          val withNumBuckets = NumBuckets(emptyState) = numBuckets
          val withShuffleColumn = ShuffleColumn(withNumBuckets) = shuffleColumnName

          When("we run the ParallelSink")
          val (finalState, result) = ParallelSink.run(withShuffleColumn)

          Then("the written partitions look as expected")
          result should have length (2)
          spark.read.parquet(result(1).filename).collect() should contain theSameElementsInOrderAs (df.collect())

          And("the final state is unchanged, but the shuffle column is gone")
          Schema(finalState) should equal (schema)
          finalState.copy(data = df) should equal (withShuffleColumn.copy(data = df))
        }
      }
    }
  }
}
