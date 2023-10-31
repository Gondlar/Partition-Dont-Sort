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
import de.unikl.cs.dbis.waves.partitions.EvenNWay
import de.unikl.cs.dbis.waves.partitions.Bucket

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
    "not be constructable for non-percentage values of maxBucketSize" in {
      an [AssertionError] shouldBe thrownBy (ModelGini(-1, .5, false))
      an [AssertionError] shouldBe thrownBy (ModelGini(2, .5, false))
    }
    "not be constructable for non-percentage values of minBucketSize" in {
      an [AssertionError] shouldBe thrownBy (ModelGini(.5, -1, false))
      an [AssertionError] shouldBe thrownBy (ModelGini(.5, 2, false))
    }
    "not be constructable if minBucketSize >= maxBucketSize" in {
      an [AssertionError] shouldBe thrownBy (ModelGini(.5, .8, false))
      an [AssertionError] shouldBe thrownBy (ModelGini(.5, .5, false))
    }
    "not be supported when no VersionTree is given in the state" in {
      val step = ModelGini(.5)
      (step supports dummyState) shouldBe (false)
    }
    "be supported when an VersionTree is given in the state" in {
      val step = ModelGini(.5)
      val state = StructureMetadata(dummyState) = graphForDf
      (step supports state) shouldBe (true)
    }
    "split the dataset according to the best gini gain" in {
      Given("A state and a number of partitions")
      val state = StructureMetadata(dummyDfState) = graphForDf
      val step = ModelGini(.25)

      When("we apply the ExactGini step")
      val result = step(state)

      Then("the correct shape is stored")
      Shape(result) should equal (
        SplitByPresence(PathKey("b"),
          SplitByPresence("b.d", (), ()),
          SplitByPresence("a", (), ())
        )
      )
      NumBuckets.get(result).value should equal (4)

      And("the buckets have the right data")
      val buckets = Buckets(result)
      buckets should have length (4)
      buckets(0).collect should contain theSameElementsAs (df.filter(col("b").isNull && col("a").isNull).collect())
      buckets(1).collect should contain theSameElementsAs (df.filter(col("b").isNull && col("a").isNotNull).collect())
      buckets(2).collect should contain theSameElementsAs (df.filter(col("b").isNotNull && col("b.d").isNull).collect())
      buckets(3).collect should contain theSameElementsAs (df.filter(col("b").isNotNull && col("b.d").isNotNull).collect())
    }
    "create n-way splits if no others are eligible" in {
      Given("A state and a very high bound")
      val state = StructureMetadata(dummyDfState) = graphForDf
      val step = ModelGini(.99, .98, false)

      When("we apply the ExactGini step")
      val result = step(state)

      Then("the correct shape is stored")
      Shape(result) should equal (EvenNWay(IndexedSeq(Bucket(()), Bucket(()))))
      NumBuckets.get(result).value should equal (2)
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
      val step = ModelGini(.25)

      noException shouldBe thrownBy (step(state))
    }
  }
}