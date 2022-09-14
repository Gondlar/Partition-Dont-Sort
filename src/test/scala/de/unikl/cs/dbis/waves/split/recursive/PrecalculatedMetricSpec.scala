package de.unikl.cs.dbis.waves.split.recursive

import de.unikl.cs.dbis.waves.WavesSpec

import de.unikl.cs.dbis.waves.SchemaFixture
import org.apache.spark.sql.types.{StructType, StructField, BooleanType}
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.ArrayType

class PrecalculatedMetricSpec extends WavesSpec
    with SchemaFixture {
    
    "The PrecalculatedMetric" should {
        "copy the values from the given array in the Row" in {
            Given("A row with a array and a counter")
            val schema = StructType(Seq(StructField("foo", ArrayType(BooleanType, false))))
            val row = new GenericRowWithSchema(Array(Seq(true, false, true, true)), schema)
            val counter = ObjectCounter(4)

            Then("all counts are correct")
            counter <-- PrecalculatedMetric(row, row.fieldIndex("foo"), 5)
            counter.values should equal (Array(5, 0, 5, 5))
        }
        "work if there are no optional objects" in {
            Given("A row with a array and a counter")
            val schema = StructType(Seq(StructField("foo", ArrayType(BooleanType, false))))
            val row = new GenericRowWithSchema(Array(Seq.empty), schema)
            val counter = ObjectCounter(0)

            Then("nothing has changed")
            counter <-- PrecalculatedMetric(row, row.fieldIndex("foo"), 5)
            counter.values should have length 0
        }
        "throw an AssertionError if the size does not match" in {
            Given("A row with a array and a counter")
            val schema = StructType(Seq(StructField("foo", ArrayType(BooleanType, false))))
            val row = new GenericRowWithSchema(Array(Seq(true, false, true, true)), schema)
            val tooSmall = ObjectCounter(3)
            val tooBig = ObjectCounter(3)

            Then("errors are thrown")
            an [AssertionError] shouldBe thrownBy (tooSmall <-- PrecalculatedMetric(row, row.fieldIndex("foo"), 5))
            an [AssertionError] shouldBe thrownBy (tooBig <-- PrecalculatedMetric(row, row.fieldIndex("foo"), 5))
        }
    }
}
