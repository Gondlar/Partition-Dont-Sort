package de.unikl.cs.dbis.waves.pipeline.util

import org.scalatest.Inspectors._
import de.unikl.cs.dbis.waves.WavesSpec
import de.unikl.cs.dbis.waves.DataFrameFixture

import de.unikl.cs.dbis.waves.pipeline._
import de.unikl.cs.dbis.waves.partitions.Bucket
import de.unikl.cs.dbis.waves.partitions.SplitByPresence

class ShuffleByShapeSpec extends WavesSpec
  with DataFrameFixture {

  "The ShuffleByShape Step" when {
    "no shape is given" should {
      "not be supported" in {
        (ShuffleByShape supports PipelineState(null, null)) shouldBe (false)
      }
    }
    "a shape is given" should {
      "be supported" in {
        (ShuffleByShape supports (Shape(PipelineState(null, null)) = Bucket(()))) shouldBe (true)
      }
      "shuffle the data according to the shape" in {
        Given("A state with buckets")
        val state = Shape(PipelineState(df, null)) = SplitByPresence("a", (), ())

        When("we apply the FlatShapeBuilder step")
        val result = ShuffleByShape(state)

        Then("the correct shape is stored")
        df.rdd.getNumPartitions should equal (2)

        val valuesPerPartition = df.rdd.mapPartitions{ iter => 
          Iterator(iter.map(_.isNullAt(0)).toSet)
        }.collect()
        valuesPerPartition should have length (2)
        valuesPerPartition(0).intersect(valuesPerPartition(1)) shouldBe empty
        valuesPerPartition(0).union(valuesPerPartition(1)) should equal (Set(false, true))
      }
    }
  }
}
