package de.unikl.cs.dbis.waves.pipeline.util

import org.scalatest.Inspectors._
import de.unikl.cs.dbis.waves.WavesSpec
import de.unikl.cs.dbis.waves.DataFrameFixture

import de.unikl.cs.dbis.waves.pipeline._

import org.apache.spark.sql.functions.{col,when,spark_partition_id,count,count_distinct}

class ShuffleSpec extends WavesSpec
  with DataFrameFixture with PipelineStateFixture {

  "The Shuffle Step" when {
    "no shuffle column is given" should {
      "not be supported" in {
        (Shuffle supports dummyState) shouldBe (false)
      }
    }
    "no number of buckets is given" should {
      "not be supported" in {
        val withShuffleColumn = ShuffleColumn(dummyState) = "test"
        (Shuffle supports withShuffleColumn) shouldBe (false)
      }
    }
    "a shuffle column and number of buckets is given" should {
      "be supported" in {
        val withShuffleColumn = ShuffleColumn(dummyState) = "test"
        val withNumBuckets = NumBuckets(withShuffleColumn) = 2
        (Shuffle supports withNumBuckets) shouldBe (true)
      }
      "shuffle the data according to the shuffle column" in {
        Given("A state with a shuffle column")
        val shuffleColumnName = "shuffle"
        val numBuckets = 2

        val shuffledDf = df.withColumn(shuffleColumnName, when(col("a").isNull, 0).otherwise(1))
        val withShuffleColumn = ShuffleColumn(PipelineState(shuffledDf, null)) = shuffleColumnName
        val withNumBuckets = NumBuckets(withShuffleColumn) = numBuckets

        When("we apply the Shuffle step")
        val result = Shuffle(withNumBuckets)

        Then("there is one partition per value in the shuffle column")
        val counts = result.data
          .groupBy(spark_partition_id)
          .agg(count("*"), count_distinct(col(shuffleColumnName)))
          .collect()

        counts should have length (2)
        forAll(counts) { row =>
          row.getLong(1) should equal (4)
          row.getLong(2) should equal (1)
        }
      }
    }
  }
}
