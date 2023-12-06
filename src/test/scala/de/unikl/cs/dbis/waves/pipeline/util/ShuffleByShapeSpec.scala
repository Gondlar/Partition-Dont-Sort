package de.unikl.cs.dbis.waves.pipeline.util

import org.scalatest.Inspectors._
import de.unikl.cs.dbis.waves.WavesSpec
import de.unikl.cs.dbis.waves.DataFrameFixture

import de.unikl.cs.dbis.waves.pipeline._
import de.unikl.cs.dbis.waves.partitions.Bucket
import de.unikl.cs.dbis.waves.partitions.SplitByPresence

import org.apache.spark.sql.functions.{col,when}

class ShuffleByShapeSpec extends WavesSpec
  with DataFrameFixture with PipelineStateFixture {

  "The ShuffleByShape Step" when {
    "no shape is given" should {
      "not be supported" in {
        (ShuffleByShape supports dummyState) shouldBe (false)
      }
    }
    "a shuffle column is already defined" should {
      "not be supported" in {
        val withShape = Shape(dummyState) = Bucket(())
        val withShuffleColumn = ShuffleColumn(withShape) = "test"
        (ShuffleByShape supports withShuffleColumn) shouldBe (false)
      }
    }
    "a shape is given" should {
      "be supported" in {
        (ShuffleByShape supports (Shape(dummyState) = Bucket(()))) shouldBe (true)
      }
      "shuffle the data according to the shape" in {
        Given("A state with buckets")
        val state = Shape(dummyDfState) = SplitByPresence("a", (), ())

        When("we apply the ShuffleByShape step")
        val result = ShuffleByShape(state)

        Then("there is a shuffle column")
        (ShuffleColumn isDefinedIn result) shouldBe (true)
        val shuffleColumn = ShuffleColumn(result)

        And("that column has the correct values")
        result.data.count() should equal (df.count())
        val invalidRows = result.data
          .filter((col(shuffleColumn) =!= when(col("a").isNull, 0).otherwise(1)).as("test"))
          .count()
        invalidRows should equal (0)
      }
    }
  }
}
