package de.unikl.cs.dbis.waves.pipeline.sort

import de.unikl.cs.dbis.waves.WavesSpec

import org.apache.spark.sql.functions.col

import de.unikl.cs.dbis.waves.pipeline._
import de.unikl.cs.dbis.waves.split.recursive.ColumnMetadata
import de.unikl.cs.dbis.waves.split.recursive.RSIGraph
import de.unikl.cs.dbis.waves.util.PathKey

class RSIGraphCardinalitiesSpec extends WavesSpec
  with PipelineStateFixture {

  "The RSIGraphCardinalities Orderer" should {
    "require a graph to be supported" in {
      (RSIGRaphCardinalities supports dummyState) shouldBe (false)
    }
    "be supported if a graph is defined" in {
      val state = StructureMetadata(dummyState) = RSIGraph.empty
      (RSIGRaphCardinalities supports state) shouldBe (true)
    }
    "return the columns in increasing cardinality order" in {
      Given("a state with a graph")
      val graph = RSIGraph(
        ("a", .5, RSIGraph(leafMetadata = Some(ColumnMetadata(0, 9, 5)))),
        ("b", .2, RSIGraph(
          ("c", 1d, RSIGraph.empty),
          ("d", .8, RSIGraph.empty)
        )),
        ("e", 0, RSIGraph.empty)
      )
      val state = StructureMetadata(dummyState) = graph

      When("we get the column order")
      val result = RSIGRaphCardinalities.sort(state, null)
      
      Then("it should be correct")
      val order = Seq(PathKey("a"), PathKey("b.c"), PathKey("b.d"))
        .map(ExactCardinalities.definitionLevel(_)) :+ col("a")
      result should contain theSameElementsInOrderAs (order)
    }
  }
}