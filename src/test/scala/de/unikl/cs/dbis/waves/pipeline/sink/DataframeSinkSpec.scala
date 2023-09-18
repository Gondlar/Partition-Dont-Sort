package de.unikl.cs.dbis.waves.pipeline.sink

import org.scalatest.Inspectors._
import de.unikl.cs.dbis.waves.WavesSpec
import de.unikl.cs.dbis.waves.RelationFixture
import de.unikl.cs.dbis.waves.TempFolderFixture

import org.apache.spark.sql.functions.col

import de.unikl.cs.dbis.waves.WavesTable._
import de.unikl.cs.dbis.waves.pipeline._

class DataframeSinkSpec extends WavesSpec
  with RelationFixture with TempFolderFixture with PipelineStateFixture {

  "A DataframeSink" when {
    "buckets are undefined" should {
      "not be supported" in {
        (DataframeSink supports dummyState) shouldBe (false)
      }
    }
    "buckets are defined" should {
      "be supported" in {
        val withBuckets = Buckets(dummyState) = Seq()
        (DataframeSink supports withBuckets) shouldBe (true)
      }
      "require finalization" in {
        (DataframeSink isAlwaysFinalizedFor dummyState) shouldBe (false)
      }
      "store each bucket as a Partition" when {
        "there are multiple buckets" in {
          Given("A PipelineState with Buckets")
          val emptyState = PipelineState(df, tempDirectory)
          val withBuckets = Buckets(emptyState) = Seq(df.filter(col("a").isNull), df.filter(col("a").isNotNull))

          When("we run the DataframeSink")
          val (finalState, result) = DataframeSink.run(withBuckets)

          Then("the written partitions look as expected")
          result should have length (2)
          spark.read.parquet(result(0).filename).collect() should contain theSameElementsInOrderAs (df.filter(col("a").isNull).collect())
          spark.read.parquet(result(1).filename).collect() should contain theSameElementsInOrderAs (df.filter(col("a").isNotNull).collect())

          And("the final state is unchanged")
          finalState should equal (withBuckets)
        }
        "there is only one bucket" in {
          Given("A PipelineState with one Bucket")
          val emptyState = PipelineState(df, tempDirectory)
          val withBuckets = Buckets(emptyState) = Seq(df)

          When("we run the DataframeSink")
          val (finalState, result) = DataframeSink.run(withBuckets)

          Then("the written partitions look as expected")
          result should have length (1)
          spark.read.parquet(result(0).filename).collect() should contain theSameElementsInOrderAs (df.collect())

          And("the final state is unchanged")
          finalState should equal (withBuckets)
        }
      }
    }
  }
}
