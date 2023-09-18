package de.unikl.cs.dbis.waves.pipeline.util

import de.unikl.cs.dbis.waves.WavesSpec
import de.unikl.cs.dbis.waves.DataFrameFixture
import de.unikl.cs.dbis.waves.PartitionTreeFixture
import org.scalatest.Inspectors._

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.functions.{col, struct}
import org.apache.spark.sql.types.{StructType, StructField, IntegerType}

import de.unikl.cs.dbis.waves.partitions.visitors.operations._
import de.unikl.cs.dbis.waves.pipeline._
import de.unikl.cs.dbis.waves.partitions.Bucket
import de.unikl.cs.dbis.waves.partitions.SplitByPresence
import de.unikl.cs.dbis.waves.partitions.PartitionMetadata
import de.unikl.cs.dbis.waves.partitions.Absent
import de.unikl.cs.dbis.waves.util.PathKey


class SchemaModifierSpec extends WavesSpec
  with DataFrameFixture with PartitionTreeFixture with PipelineStateFixture {

  "The SchemaModifier Step" should {
    "not be supported" when {
      "only buckets are given" in {
        (SchemaModifier supports (Buckets(dummyState) = Seq())) should be (false)
      }
      "only a shape is given" in {
        (SchemaModifier supports (Shape(dummyState) = Bucket(()))) should be (false)
      }
      "nothing is given" in {
        (SchemaModifier supports dummyState) should be (false)
      }
    }
    "be supported" when {
      "buckets and shape are given" in {
        val withBuckets = Buckets(dummyState) = Seq()
        val withBoth = Shape(withBuckets) = Bucket(())
        (SchemaModifier supports withBoth) shouldBe (true)
      }
    }
    "modify the schemas correctly" when {
      "the key ist nested" in {
        Given("Buckets and their shape")
        val withShape = Shape(dummyState) = split.shape
        val buckets = Seq(df.filter(col("b.d").isNull), df.filter(col("b.d").isNotNull))
        val withBoth = Buckets(withShape) = buckets

        When("we apply the SchemaModifier")
        val result = SchemaModifier.run(withBoth)

        Then("the schemas are correct")
        (Buckets isDefinedIn result) shouldBe (true)
        val modifiedBuckets = Buckets(result)
        modifiedBuckets should have length (2)

        val reqSchema = StructType(Seq( StructField("a", IntegerType, true)
                                      , StructField("b", StructType(Seq( StructField("c", IntegerType, false)
                                                                       , StructField("d", IntegerType, false)
                                                                       )), false)
                                      , StructField("e", IntegerType, false)
                                      ))
        val absSchema = StructType(Seq( StructField("a", IntegerType, true)
                                      , StructField("b", StructType(Seq( StructField("c", IntegerType, false)
                                                                       )), true)
                                      , StructField("e", IntegerType, false)
                                      ))

        modifiedBuckets(0).schema should equal (absSchema)
        modifiedBuckets(1).schema should equal (reqSchema)

        And("the contents are otherwise unmodified")
        modifiedBuckets(0).collect() should contain theSameElementsInOrderAs (buckets(0).select(col("a"), col("b").dropFields("d"), col("e")).collect())
        modifiedBuckets(1).collect() should contain theSameElementsInOrderAs (buckets(1).collect())
      }
      "the key is flat" in {
        Given("Buckets and their shape")
        val withShape = Shape(dummyState) = SplitByPresence("b", (), ())
        val buckets = Seq(df.filter(col("b").isNull), df.filter(col("b").isNotNull))
        val withBoth = Buckets(withShape) = buckets

        When("we apply the SchemaModifier")
        val result = SchemaModifier.run(withBoth)

        Then("the schemas are correct")
        (Buckets isDefinedIn result) shouldBe (true)
        val modifiedBuckets = Buckets(result)
        modifiedBuckets should have length (2)

        val reqSchema = StructType(Seq( StructField("a", IntegerType, true)
                                      , StructField("b", StructType(Seq( StructField("c", IntegerType, false)
                                                                       , StructField("d", IntegerType, true)
                                                                       )), false)
                                      , StructField("e", IntegerType, false)
                                      ))
        val absSchema = StructType(Seq( StructField("a", IntegerType, true)
                                      , StructField("e", IntegerType, false)
                                      ))

        modifiedBuckets(0).schema should equal (absSchema)
        modifiedBuckets(1).schema should equal (reqSchema)

        And("the contents are otherwise unmodified")
        modifiedBuckets(0).collect() should contain theSameElementsInOrderAs (buckets(0).drop(col("b")).collect())
        modifiedBuckets(1).collect() should contain theSameElementsInOrderAs (buckets(1).collect())
      }
      "the key is three levels nested" in {
        Given("Buckets and their shape")
        val data = df.select(struct(df.columns.head, df.columns.tail:_*).as("foo"))
        val withShape = Shape(dummyState) = SplitByPresence("foo.b.d", (), ())
        val buckets = Seq(data.filter(col("foo.b.d").isNull), data.filter(col("foo.b.d").isNotNull))
        val withBoth = Buckets(withShape) = buckets

        When("we apply the SchemaModifier")
        val result = SchemaModifier.run(withBoth)

        Then("the schemas are correct")
        (Buckets isDefinedIn result) shouldBe (true)
        val modifiedBuckets = Buckets(result)
        modifiedBuckets should have length (2)

        val reqSchema = StructType(Seq(StructField( "foo", StructType(Seq( StructField("a", IntegerType, true)
                                                                         , StructField("b", StructType(Seq( StructField("c", IntegerType, false)
                                                                                                          , StructField("d", IntegerType, false)
                                                                                                          )), false)
                                                                         , StructField("e", IntegerType, false)
                                                                         )), false)
                                      ))
                        
        val absSchema = StructType(Seq(StructField( "foo", StructType(Seq( StructField("a", IntegerType, true)
                                                                         , StructField("b", StructType(Seq( StructField("c", IntegerType, false)
                                                                                                          )), true)
                                                                         , StructField("e", IntegerType, false)
                                                                         )), false)
                                      ))

        modifiedBuckets(0).schema should equal (absSchema)
        modifiedBuckets(1).schema should equal (reqSchema)

        And("the contents are otherwise unmodified")
        modifiedBuckets(0).collect() should contain theSameElementsInOrderAs (buckets(0).select(col("foo").dropFields("b.d")).collect())
        modifiedBuckets(1).collect() should contain theSameElementsInOrderAs (buckets(1).collect())
      }
      "there is known metadata" in {
        Given("Buckets and their shape")
        val withShape = Shape(dummyState) = SplitByPresence("b", (), ())
        val buckets = Seq(df.filter(col("b").isNull && col("a").isNull), df.filter(col("b").isNotNull && col("a").isNull))
        val withBoth = Buckets(withShape) = buckets
        val withMetadata = KnownMetadata(withBoth) = PartitionMetadata(Seq.empty, Seq(PathKey("a")), Seq(Absent))

        When("we apply the SchemaModifier")
        val result = SchemaModifier.run(withMetadata)

        Then("the schemas are correct")
        (Buckets isDefinedIn result) shouldBe (true)
        val modifiedBuckets = Buckets(result)
        modifiedBuckets should have length (2)

        val reqSchema = StructType(Seq( StructField("b", StructType(Seq( StructField("c", IntegerType, false)
                                                                       , StructField("d", IntegerType, true)
                                                                       )), false)
                                      , StructField("e", IntegerType, false)
                                      ))
        val absSchema = StructType(Seq( StructField("e", IntegerType, false)
                                      ))

        modifiedBuckets(0).schema should equal (absSchema)
        modifiedBuckets(1).schema should equal (reqSchema)

        And("the contents are otherwise unmodified")
        modifiedBuckets(0).collect() should contain theSameElementsInOrderAs (buckets(0).drop(col("b")).drop(col("a")).collect())
        modifiedBuckets(1).collect() should contain theSameElementsInOrderAs (buckets(1).drop(col("a")).collect())
      }
    }
  }
}