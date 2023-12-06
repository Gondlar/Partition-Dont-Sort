package de.unikl.cs.dbis.waves.pipeline.util

import org.scalatest.Inspectors._
import de.unikl.cs.dbis.waves.WavesSpec
import de.unikl.cs.dbis.waves.DataFrameFixture

import de.unikl.cs.dbis.waves.pipeline._

import org.apache.spark.sql.functions.{col,when,spark_partition_id}

class PreshuffledSpec extends WavesSpec
  with DataFrameFixture with PipelineStateFixture {

  "The Preshuffled Step" when {
    "a shuffle column is already defined" should {
      "not be supported" in {
        val withShuffleColumn = ShuffleColumn(dummyState) = "test"
        (Preshuffled supports withShuffleColumn) shouldBe (false)
      }
    }
    "a no shuffle column is given" should {
      "be supported" in {
        (Preshuffled supports dummyState) shouldBe (true)
      }
      "record how the data is currently shuffled" in {
        Given("A preshuffled state")
        // there is a hash collision between 0 and 1
        val shuffledDf = df.repartition(2, when(col("a").isNull,1).otherwise(2))
        val state = PipelineState(shuffledDf, null)

        When("we apply the Preshuffled step")
        val result = Preshuffled(state)

        Then("there is a shuffle column")
        (ShuffleColumn isDefinedIn result) shouldBe (true)
        val shuffleColumn = ShuffleColumn(result)

        And("that column has the correct values")
        result.data.count() should equal (df.count())
        val invalidRows = result.data
          .filter((col(shuffleColumn) =!= when(col("a").isNull, 1).otherwise(0)).as("test"))
          .count()
        invalidRows should equal (0)
      }
    }
  }
}
