package de.unikl.cs.dbis.waves.pipeline.util

import de.unikl.cs.dbis.waves.WavesSpec
import de.unikl.cs.dbis.waves.DataFrameFixture
import org.scalatest.matchers.Matcher
import org.scalatest.matchers.MatchResult
import org.scalatest.Inspectors._


import org.apache.hadoop.fs.FileSystem
import de.unikl.cs.dbis.waves.partitions.visitors.operations._
import de.unikl.cs.dbis.waves.pipeline._
import de.unikl.cs.dbis.waves.util.PartitionFolder

class FinalizerSpec extends WavesSpec
  with DataFrameFixture {

  "The Finalizer Step" when {
    "no buckets are given" should {
      "not be supported" in {
        (Finalizer supports PipelineState(null, null)) shouldBe (false)
      }
    }
    "buckets are given" should {
      "be supported" in {
        (Finalizer supports (Buckets(PipelineState(null, null)) = Seq())) shouldBe (true)
      }
      "merge each bucket's spark partitions" in {
        Given("A state with buckets")
        df.rdd.getNumPartitions should equal (2)
        val state = Buckets(PipelineState(null, null)) = Seq(df, df)

        When("we apply the FlatShapeBuilder step")
        val result = Finalizer(state)

        Then("the correct shape is stored")
        (Buckets isDefinedIn result) shouldBe (true)
        val buckets = Buckets(result)
        buckets should have length (2)
        forAll (buckets) { bucket =>
          bucket.rdd.getNumPartitions should equal (1)
          bucket.collect() should contain theSameElementsInOrderAs (df.collect())
        }
      }
    }
  }
}

trait FinalizedMatcher {
  class FoldersAreFinalizedMatcher(implicit fs: FileSystem) extends Matcher[String] {

    override def apply(baseDir: String): MatchResult = {
      val folders = PartitionFolder.allInDirectory(baseDir)
      val nonFinalized = folders.filter(_.parquetFiles.length > 1).toSeq
      MatchResult(nonFinalized.isEmpty,
        s"$baseDir contains ${nonFinalized.length} unfinalized Partitions: ${nonFinalized.map(_.name)}",
        s"$baseDir is finalized")
    }

  }

  def beFinalized(implicit fs: FileSystem) = new FoldersAreFinalizedMatcher
}