package de.unikl.cs.dbis.waves.pipeline.split

import de.unikl.cs.dbis.waves.WavesSpec
import de.unikl.cs.dbis.waves.DataFrameFixture
import de.unikl.cs.dbis.waves.pipeline.util.CalculateVersionTreeSpec.graphForDf

import de.unikl.cs.dbis.waves.partitions.SplitByPresence
import de.unikl.cs.dbis.waves.partitions.visitors.operations._
import de.unikl.cs.dbis.waves.pipeline._
import de.unikl.cs.dbis.waves.util.PathKey

import org.apache.spark.sql.functions.col
import de.unikl.cs.dbis.waves.util.Versions
import de.unikl.cs.dbis.waves.util.Leaf

class ModelGiniSpec extends WavesSpec
  with DataFrameFixture with PipelineStateFixture {

  "The ModelGini Step" can {
    "merge two options" when {
      "both are none" in {
        ModelGini.mergeOptions[Int](_+_)(None, None) should equal (None)
      }
      "the left side is none" in {
        ModelGini.mergeOptions[Int](_+_)(None, Some(5)) should equal (Some(5))
      }
      "the right side is none" in {
        ModelGini.mergeOptions[Int](_+_)(Some(5), None) should equal (Some(5))
      }
      "neither side is none" in {
        ModelGini.mergeOptions[Int](_+_)(Some(7), Some(5)) should equal (Some(12))
      }
    }
  }
  it should {
    "not be constructable for non-positive splits" in {
      an [IllegalArgumentException] shouldBe thrownBy (ModelGini(0))
    }
    "not be constructable for non-percentage values of minimumBucketFill" in {
      an [IllegalArgumentException] shouldBe thrownBy (ModelGini(0, -1))
      an [IllegalArgumentException] shouldBe thrownBy (ModelGini(0, 2))
    }
    "not be supported when no VersionTree is given in the state" in {
      val step = ModelGini(1)
      (step supports dummyState) shouldBe (false)
    }
    "be supported when an VersionTree is given in the state" in {
      val step = ModelGini(1)
      val state = StructureMetadata(dummyState) = graphForDf
      (step supports state) shouldBe (true)
    }
    "split the dataset according to the best gini gain" in {
      Given("A state and a number of partitions")
      val state = StructureMetadata(dummyDfState) = graphForDf
      val step = ModelGini(4)

      When("we apply the ExactGini step")
      val result = step(state)

      Then("the correct shape is stored")
      Shape(result) should equal (
        SplitByPresence(PathKey("b"),
          SplitByPresence("a", (), ()),
          SplitByPresence("a", (), ())
        )
      )
      NumBuckets.get(result).value should equal (4)

      And("the buckets have the right data")
      val buckets = Buckets(result)
      buckets should have length (4)
      buckets(0).collect should contain theSameElementsAs (df.filter(col("b").isNull && col("a").isNull).collect())
      buckets(1).collect should contain theSameElementsAs (df.filter(col("b").isNull && col("a").isNotNull).collect())
      buckets(2).collect should contain theSameElementsAs (df.filter(col("b").isNotNull && col("a").isNull).collect())
      buckets(3).collect should contain theSameElementsAs (df.filter(col("b").isNotNull && col("a").isNotNull).collect())
    }
    "not throw exceptions if column metadata is missing" in {
      val graph = Versions(
        IndexedSeq("a", "b", "e"),
        IndexedSeq(
          Leaf.empty,
          Versions(
            IndexedSeq("c", "d"),
            IndexedSeq(
              Leaf.empty,
              Leaf.empty
            ),
            Seq( (IndexedSeq(true, true), .5)
              , (IndexedSeq(true, false), .5)
              )
          ),
          Leaf.empty
        ),
        Seq( (IndexedSeq(true, true, true), .25)
          , (IndexedSeq(true, false, true), .25)
          , (IndexedSeq(false, true, true), .25)
          , (IndexedSeq(false, false, true), .25)
          )
      )
      val state = StructureMetadata(dummyDfState) = graph
      val step = ModelGini(4)

      noException shouldBe thrownBy (step(state))
    }
  }
}