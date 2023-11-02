package de.unikl.cs.dbis.waves.pipeline.sort

import de.unikl.cs.dbis.waves.WavesSpec

import org.apache.spark.sql.functions.{col,length}

import de.unikl.cs.dbis.waves.pipeline._
import de.unikl.cs.dbis.waves.util.UniformColumnMetadata
import de.unikl.cs.dbis.waves.util.{Leaf, Versions}
import de.unikl.cs.dbis.waves.util.PathKey
import de.unikl.cs.dbis.waves.util.TotalFingerprint

class VersionTreeCardinalitiesSpec extends WavesSpec
  with PipelineStateFixture {

  "The VersionTreeCardinalities Orderer" should {
    "require a graph to be supported" in {
      (StructuralMetadataCardinalities supports dummyState) shouldBe (false)
    }
    "be supported if a graph is defined" in {
      val state = StructureMetadata(dummyState) = Leaf.empty
      (StructuralMetadataCardinalities supports state) shouldBe (true)
    }
    "return the columns in increasing cardinality order" when {
      "the graph is a version tree" in {
        Given("a state with a graph")
        val graph = Versions(
          IndexedSeq("a", "b", "e"),
          IndexedSeq(
            Leaf(Some(UniformColumnMetadata(0, 9, 5))),
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
        val result = StructuralMetadataCardinalities.sort(state, null)
        
        Then("it should be correct")
        val order = Seq(PathKey("a"), PathKey("b.c"), PathKey("b.d"))
          .map(IncreasingCardinalities.definitionLevel(_)) :+ col("a")
        result should contain theSameElementsInOrderAs (order)
      }
      "the graph is a total fingerprint" in {
        Given("a state with a graph")
        val graph = TotalFingerprint(
          IndexedSeq("a", "b", "b.c", "b.d", "e"),
          Seq(
            (IndexedSeq(true, true, true, true, false), 16),
            (IndexedSeq(true, true, true, false, false), 4),
            (IndexedSeq(true, false, false, false, false), 30),
            (IndexedSeq(false, false, false, false, false), 50),
          ),
          IndexedSeq(Some(UniformColumnMetadata(0, 9, 5)), None, None, None),
          IndexedSeq("a", "b.c", "b.d", "e"),
          IndexedSeq(50, 20, 20, 16, 0),
          100
        )
        val state = StructureMetadata(dummyState) = graph

        When("we get the column order")
        val result = StructuralMetadataCardinalities.sort(state, null)
        
        Then("it should be correct")
        val order = Seq(PathKey("a"), PathKey("b.c"), PathKey("b.d"))
          .map(IncreasingCardinalities.definitionLevel(_)) :+ col("a")
        result should contain theSameElementsInOrderAs (order)
      }
    }
  }
}