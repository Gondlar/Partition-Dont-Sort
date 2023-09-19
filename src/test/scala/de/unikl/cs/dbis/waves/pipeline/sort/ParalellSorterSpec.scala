package de.unikl.cs.dbis.waves.pipeline.sort

import de.unikl.cs.dbis.waves.WavesSpec
import de.unikl.cs.dbis.waves.SparkFixture
import org.scalatest.Inspectors._

import de.unikl.cs.dbis.waves.pipeline._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.col

class ParallelSorterSpec extends WavesSpec
  with SparkFixture with PipelineStateFixture {

  "The ParallelSorter Step" when {
    "no sortorder is defined" should {
      "not be supported" in {
        (ParallelSorter supports dummyState) shouldBe (false)
      }
    }
    "a sortorder is defined" should {
      "be supported" in {
        val state = GlobalSortorder(dummyState) = Seq(col("a"))
        (ParallelSorter supports state) shouldBe (true)
      }
      "sort each dataframe partition" in {
        Given("A dataframe and a supported pipeline state")
        val schema = StructType(Array(StructField("a", IntegerType, false)))
        val numbers = (1 to 10).reverse.map(v => new GenericRowWithSchema(Array(v), schema))
        val rdd: RDD[Row] = spark.sparkContext.parallelize(numbers, 2)
        val df = spark.sqlContext.createDataFrame(rdd, schema)

        val state = GlobalSortorder(new PipelineState(df, null)) = Seq(col("a"))

        When("we sort it")
        val result = ParallelSorter.run(state)

        Then("the partitions are sorted")
        val partitions = result.data.rdd.mapPartitions(partition => 
          Iterator(Seq(partition.map(_.getInt(0)).toSeq:_*))
        ).collect()
        partitions.flatten should contain theSameElementsAs (1 to 10)
        forAll(partitions) { partition =>
          partition shouldBe sorted
        }
      }
    }
  }
}
