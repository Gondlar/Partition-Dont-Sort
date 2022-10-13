package de.unikl.cs.dbis.waves.split

import org.scalatest.Inspectors._
import de.unikl.cs.dbis.waves.WavesSpec
import de.unikl.cs.dbis.waves.{DataFrameFixture, PartitionTreeFixture, TempFolderFixture}

import de.unikl.cs.dbis.waves.partitions.PartitionMetadata
import org.apache.spark.sql.functions.col

import Transform._
import de.unikl.cs.dbis.waves.util.PathKey
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.IntegerType

class TransformSpec extends WavesSpec
with DataFrameFixture {

  "The clipSchema Transform" should {
    "not change a Dataframe for empty Metadata" in {
      val result = df.transform(clipSchema(PartitionMetadata())) 
      result.schema should equal (df.schema)
      result.collect() should contain theSameElementsInOrderAs (df.collect())
    }
    "adjust the schema according to the metadata" in {
      Given("a dataframe")
      val test = df.filter(col("b").isNull && col("a").isNotNull)
      
      And("fitting metadata")
      val metadata = PartitionMetadata(Seq(PathKey("a")), Seq(PathKey("b")), Seq.empty)

      When("we transform the dataframe")
      val result = test.transform(clipSchema(metadata))

      Then("the schema is changed")
      val expectedSchema = StructType(Seq( StructField("a", IntegerType,false)
                                         , StructField("e",IntegerType,false)
                                         ))
      result.schema should equal (expectedSchema)
      result.collect() should contain theSameElementsInOrderAs (test.drop("b").collect())
    }
  }
  "The conditionally Transform" when {
    "its condition parameter is true" should {
      "apply the transform" in {
        val transformed = df.transform(conditionally(true, _.filter(col("b").isNull)))
        transformed.collect() should contain theSameElementsAs (df.filter(col("b").isNull).collect())
      }
    }
    "its condition parameter is false" should {
      "change nothing" in {
        val transformed = df.transform(conditionally(false, _.filter(col("b").isNull)))
        transformed.collect() should contain theSameElementsAs (df.collect())
      }
    }
  }
}
