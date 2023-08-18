package de.unikl.cs.dbis.waves.pipeline.split

import de.unikl.cs.dbis.waves.WavesSpec
import de.unikl.cs.dbis.waves.DataFrameFixture

import de.unikl.cs.dbis.waves.partitions.SplitByPresence
import de.unikl.cs.dbis.waves.partitions.visitors.operations._
import de.unikl.cs.dbis.waves.pipeline._
import de.unikl.cs.dbis.waves.util.PathKey

import org.apache.spark.sql.functions.col
import de.unikl.cs.dbis.waves.split.recursive.RSIGraph

class ModelGiniSpec extends WavesSpec
  with DataFrameFixture {
  val calculate = afterWord("calculate")

  "The ModelGini Step" can calculate {
    "the RSIGraph for a DataFrame" when {
      "the DataFrame is non-empty" in {
        ModelGini.dfToRSIGraph(df, schema) should equal (
          RSIGraph(
            ("a", .5, RSIGraph.empty),
            ("b", .5, RSIGraph(
              ("c", 1, RSIGraph.empty),
              ("d", .5, RSIGraph.empty)
            )),
            ("e", 1, RSIGraph.empty)
          )
        )
      }
      "the DataFrame is empty" in {
        ModelGini.dfToRSIGraph(emptyDf, schema) should equal (
          RSIGraph(
            ("a", 0, RSIGraph.empty),
            ("b", 0, RSIGraph(
              ("c", 1, RSIGraph.empty),
              ("d", 0, RSIGraph.empty)
            )),
            ("e", 1, RSIGraph.empty)
          )
        )
      }
    }
  }
  it can {
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
    "always be supported" in {
      val step = ModelGini(1)
      val state = PipelineState(null, null)
      (step supports state) shouldBe (true)
    }
    "split the dataset according to the best gini gain" in {
      Given("A state and a number of partitions")
      val state = PipelineState(df, null)
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