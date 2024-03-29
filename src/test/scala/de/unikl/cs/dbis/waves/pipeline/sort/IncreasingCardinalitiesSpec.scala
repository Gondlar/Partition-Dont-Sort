package de.unikl.cs.dbis.waves.pipeline.sort

import de.unikl.cs.dbis.waves.WavesSpec
import de.unikl.cs.dbis.waves.DataFrameFixture

import org.apache.spark.sql.functions.col

import de.unikl.cs.dbis.waves.pipeline._
import de.unikl.cs.dbis.waves.util.PathKey

class IncreasingCardinalitiesSpec extends WavesSpec
  with DataFrameFixture with PipelineStateFixture {

  "The ExactCardinalities Orderer" should {
    "always be supported" in {
      (ExactCardinalities() supports dummyState) shouldBe (true)
    }
    "return the columns in increasing cardinality order" in {
      val order = Seq(PathKey("a"), PathKey("b.c"), PathKey("b.d"))
        .map(IncreasingCardinalities.definitionLevel(_))
      ExactCardinalities().sort(null, df) should contain theSameElementsInOrderAs (order)
    }
  }

  "The EstimatedCardinalities Orderer" should {
    "always be supported" in {
      (EstimatedCardinalities() supports dummyState) shouldBe (true)
    }
    "return the columns in increasing cardinality order" in {
      val order = Seq(PathKey("a"), PathKey("b.c"), PathKey("b.d"))
        .map(IncreasingCardinalities.definitionLevel(_))
      EstimatedCardinalities().sort(null, df) should contain theSameElementsInOrderAs (order)
    }
  }
}