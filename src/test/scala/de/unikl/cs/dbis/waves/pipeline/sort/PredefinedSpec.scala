package de.unikl.cs.dbis.waves.pipeline.sort

import de.unikl.cs.dbis.waves.WavesSpec

import org.apache.spark.sql.functions.col

import de.unikl.cs.dbis.waves.pipeline._

class PredefinedSpec extends WavesSpec with PipelineStateFixture {

  "The Predefined Orderer" should {
    "always be supported" in {
      (Predefined(Seq()) supports dummyState) shouldBe (true)
    }
    "return the predefined order" in {
      val order = Seq(col("a"), col("b.c"))
      Predefined(order).sort(null, null) should contain theSameElementsInOrderAs (order)
    }
  }
}