package de.unikl.cs.dbis.waves.pipeline.util

import de.unikl.cs.dbis.waves.WavesSpec
import de.unikl.cs.dbis.waves.DataFrameFixture

import de.unikl.cs.dbis.waves.split.recursive.ColumnMetadata
import de.unikl.cs.dbis.waves.split.recursive.RSIGraph
import de.unikl.cs.dbis.waves.partitions.SplitByPresence
import de.unikl.cs.dbis.waves.partitions.visitors.operations._
import de.unikl.cs.dbis.waves.pipeline._
import de.unikl.cs.dbis.waves.util.PathKey

import org.apache.spark.sql.functions.col

class CalculateRSIGraphSpec extends WavesSpec
  with DataFrameFixture {
    import CalculateRSIGraphSpec._

  "The CalculateRSIGraph Step" can {
    "calculate the RSIGraph for a DataFrame" when {
      "the DataFrame is non-empty" in {
        CalculateRSIGraph.dfToRSIGraph(df, schema) should equal (graphForDf)
      }
      "the DataFrame is empty" in {
        CalculateRSIGraph.dfToRSIGraph(emptyDf, schema) should equal (
          RSIGraph(
            ("a", 0, RSIGraph.empty),
            ("b", 0, RSIGraph(
              ("c", 1d, RSIGraph.empty),
              ("d", 0d, RSIGraph.empty)
            )),
            ("e", 1, RSIGraph.empty)
          )
        )
      }
    }
  }
  it should {
    "always be supported" in {
      val state = PipelineState(null, null)
      (CalculateRSIGraph supports state) shouldBe (true)
    }
    "calculate the correct RSIGraph for the state" in {
      Given("A state")
      val state = PipelineState(df, null)

      When("we apply the CalculateGSIGraph step")
      val result = CalculateRSIGraph(state)

      Then("the correct RSIGraph is stored")
      (StructureMetadata isDefinedIn result) shouldBe (true)
      StructureMetadata(result) should equal (graphForDf)
    }
  }
}

object CalculateRSIGraphSpec {
  val graphForDf = RSIGraph(
    ("a", .5, RSIGraph(leafMetadata = Some(ColumnMetadata(5,5,1)))),
    ("b", .5, RSIGraph(
      ("c", 1d, RSIGraph(leafMetadata = Some(ColumnMetadata(1,1,1)))),
      ("d", .5, RSIGraph(leafMetadata = Some(ColumnMetadata(5,5,1))))
    )),
    ("e", 1, RSIGraph(leafMetadata = Some(ColumnMetadata(42,42,1))))
  )
}