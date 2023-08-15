package de.unikl.cs.dbis.waves.pipeline.split

import de.unikl.cs.dbis.waves.WavesSpec
import de.unikl.cs.dbis.waves.DataFrameFixture

import de.unikl.cs.dbis.waves.partitions.SplitByPresence
import de.unikl.cs.dbis.waves.partitions.visitors.operations._
import de.unikl.cs.dbis.waves.pipeline._
import de.unikl.cs.dbis.waves.util.PathKey

import org.apache.spark.sql.functions.col

class ExactGiniSpec extends WavesSpec
  with DataFrameFixture {
  val calculate = afterWord("calculate")

  "The ExactGini Step" can calculate {
    "the Gini index for a DataFrame" when {
      "the DataFrame is non-empty" in {
        ExactGini.calculateGini(df) should have (
          'size (8),
          'gini (1.625)
        )
      }
      "the DataFrame is empty" in {
        an [IllegalArgumentException] shouldBe thrownBy (ExactGini.calculateGini(emptyDf))
      }
    }
    "the gain of a definition level split" when {
      "the DataFrame is non-empty" in {
        val key = PathKey("a")
        val path = Seq.empty
        val res = ExactGini.calculateDefinitionLevelSplit(df, BucketInfo(8, 1.625), path, key)
        res shouldBe 'defined
        res.get should have (
          'absentInfo (BucketInfo(4, 1.125)),
          'presentInfo (BucketInfo(4, 1.125)),
          'priority (0.5),
          'path (path),
          'key (key)
        )
      }
      "the DataFrame is empty" in {
        val key = PathKey("a")
        val path = Seq.empty
        val res = ExactGini.calculateDefinitionLevelSplit(emptyDf, BucketInfo(8, 1.625), path, key)
        res should not be 'defined
      }
    }
    "the best split" when {
      "the DataFrame is non-empty" in {
        val path = Seq.empty
        val res = ExactGini.findBestSplit(df, BucketInfo(8, 1.625), path)
        res shouldBe 'defined
        res.get should have (
          'absentInfo (BucketInfo(4, 0.5)),
          'presentInfo (BucketInfo(4, 1.0)),
          'priority (0.875),
          'path (path),
          'key (PathKey("b"))
        )
      }
      "the DataFrame is empty" in {
        val path = Seq.empty
        val res = ExactGini.findBestSplit(emptyDf, BucketInfo(8, 1.625), path)
        res should not be 'defined
      }
    }
  }
  it should {
    "not be constructable for non-positive splits" in {
      an [IllegalArgumentException] shouldBe thrownBy (ExactGini(0))
    }
    "always be supported" in {
      val step = ExactGini(1)
      val state = PipelineState(null, null)
      (step supports state) shouldBe (true)
    }
    "split the dataset according to the best gini gain" in {
      Given("A state and a number of partitions")
      val state = PipelineState(df, null)
      val step = ExactGini(4)

      When("we apply the ExactGini step")
      val result = step(state)

      Then("the correct shape is stored")
      Shape(result) should equal (
        SplitByPresence(PathKey("b"),
          SplitByPresence("a", (), ()),
          SplitByPresence("a", (), ())
        )
      )

      And("the buckets have the right data")
      val buckets = Buckets(result)
      buckets should have length (4)
      buckets(0).collect should contain theSameElementsAs (df.filter(col("b").isNull && col("a").isNull).collect())
      buckets(1).collect should contain theSameElementsAs (df.filter(col("b").isNull && col("a").isNotNull).collect())
      buckets(2).collect should contain theSameElementsAs (df.filter(col("b").isNotNull && col("a").isNull).collect())
      buckets(3).collect should contain theSameElementsAs (df.filter(col("b").isNotNull && col("a").isNotNull).collect())
    }
  }
}