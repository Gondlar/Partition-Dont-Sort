package de.unikl.cs.dbis.waves.autosplit

import de.unikl.cs.dbis.waves.WavesSpec

import de.unikl.cs.dbis.waves.Schema
import org.apache.spark.sql.types.{StructType, StructField, IntegerType}

class LeafMetricSpec extends WavesSpec
    with Schema {
    
    "The LeafMetric" should {
        "count the number of leafs under an object" in {
            Given("A schema with optional objects")
            val metric = LeafMetric(schema)
            val counter = ObjectCounter(schema)

            Then("all counts are correct")
            counter <-- metric
            counter.values should equal (Array(1,2,1))

            And("the schema is unchanged")
            // schemas are immuatble
            metric.subject should equal (schema)
        }
        "work if there are no optional objects" in {
            Given("A schema with no optional objects")
            val metric = LeafMetric(schemaNonoptional)
            val counter = ObjectCounter(schemaNonoptional)

            Then("nothing has changed")
            counter <-- metric
            counter.values should have length 0
            metric.subject should equal (schemaNonoptional)
        }
    }
}
