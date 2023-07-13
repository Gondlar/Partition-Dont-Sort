package de.unikl.cs.dbis.waves.pipeline.sort

import de.unikl.cs.dbis.waves.WavesSpec

import org.apache.spark.sql.functions.col

import de.unikl.cs.dbis.waves.pipeline._

class PredefinedSpec extends WavesSpec  {

  "The Predefined Orderer" should {
    "always be supported" in {
      val state = PipelineState(null,null)
      (Predefined(Seq()) supports state) shouldBe (true)
    }
    "return the predefined order" in {
      val order = Seq(col("a"), col("b.c"))
      Predefined(order).sort(null, null) should contain theSameElementsInOrderAs (order)
    }
  }
}