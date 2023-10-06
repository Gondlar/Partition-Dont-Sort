package de.unikl.cs.dbis.waves.pipeline.sort

import de.unikl.cs.dbis.waves.WavesSpec

import org.apache.spark.sql.functions.col

import de.unikl.cs.dbis.waves.pipeline._
import de.unikl.cs.dbis.waves.split.recursive.ColumnMetadata
import de.unikl.cs.dbis.waves.util.{Leaf, Versions}
import de.unikl.cs.dbis.waves.util.PathKey

class VersionTreeCardinalitiesSpec extends WavesSpec
  with PipelineStateFixture {

  "The VersionTreeCardinalities Orderer" should {
    "require a graph to be supported" in {
      (VersionTreeCardinalities supports dummyState) shouldBe (false)
    }
    "be supported if a graph is defined" in {
      val state = StructureMetadata(dummyState) = Leaf.empty
      (VersionTreeCardinalities supports state) shouldBe (true)
    }
    "return the columns in increasing cardinality order" in {
      Given("a state with a graph")
      val graph = Versions(
        IndexedSeq("a", "b", "e"),
        IndexedSeq(
          Leaf(Some(ColumnMetadata(0, 9, 5))),
          Versions(
            IndexedSeq("c", "d"),
            IndexedSeq(
              Leaf.empty,
              Leaf.empty
            ),
            Seq( (IndexedSeq(true, true), .8)
               , (IndexedSeq(true, false), .2)
            )
          ),
          Leaf.empty
        ),
        Seq( (IndexedSeq(true, true, false), .2)
           , (IndexedSeq(true, false, false), .3)
           , (IndexedSeq(false, false, false), .5)
           )
      )
      val state = StructureMetadata(dummyState) = graph

      When("we get the column order")
      val result = VersionTreeCardinalities.sort(state, null)
      
      Then("it should be correct")
      val order = Seq(PathKey("a"), PathKey("b.c"), PathKey("b.d"))
        .map(ExactCardinalities.definitionLevel(_)) :+ col("a")
      result should contain theSameElementsInOrderAs (order)
    }
  }
}