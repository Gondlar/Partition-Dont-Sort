package de.unikl.cs.dbis.waves.util.operators

import de.unikl.cs.dbis.waves.WavesSpec
import de.unikl.cs.dbis.waves.SparkFixture

import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{StructType, StructField, ArrayType, IntegerType}

import collection.JavaConverters._
import java.{util => ju}

class AddXSpec extends WavesSpec
    with SparkFixture {

    "The AddX Expression" when {
        "being used via Column" should {
            "add x to every array entry" in {
                val data = spark.createDataFrame(
                    List(Row(Array(-1,0,1,2))).asJava,
                    StructType(Array(StructField("foo", ArrayType(IntegerType, false))))
                )
                val res = data.select(addX(col("foo"), 5)).collect()
                res.length should equal (1)
                res(0).get(0) should equal (Array(4,5,6,7))
            }
            "return null on null input" in {
                val data = spark.createDataFrame(
                    List(Row(null : Array[Int])).asJava,
                    StructType(Array(StructField("foo", ArrayType(IntegerType, false))))
                )
                val res = data.select(addX(col("foo"), 5)).collect()
                res.length should equal (1)
                res(0).get(0) should be (null : Array[Int])
            }
            "have repeatable results" in {
                // As you have probably guessed by the nature of this test: yes, this was a bug
                Given("a data frame with an array of numbers")
                val data = spark.createDataFrame(
                    List(Row(Array(-1,0,1,2))).asJava,
                    StructType(Array(StructField("foo", ArrayType(IntegerType, false))))
                )
                When("when we add 5 componentwise")
                val applied = data.select(addX(col("foo"), 5))
                And("fetch the data twice")
                // our data has only one row, so this should return the same data
                // we only use them to make Spark evaluate the DF twice
                val res1 = applied.limit(50).collect()
                val res2 = applied.limit(5).collect()
                Then("both results are the same")
                res1 should contain theSameElementsAs (res2)
            }
        }
    }
    
}