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
        }
    }
    
}