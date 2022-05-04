package de.unikl.cs.dbis.waves.autosplit

import de.unikl.cs.dbis.waves.WavesSpec

import de.unikl.cs.dbis.waves.Schema
import org.apache.spark.sql.types.{StructType, StructField, IntegerType}

class PresentMetricSpec extends WavesSpec
    with Schema {
    
    "The PresentMetric" should {
        "measure if optional objects are present" in {
            Given("A row with optional objects")
            val metric = PresentMetric(makeEntry(true, false, true))
            val counter = ObjectCounter(schema)

            Then("all counts are correct")
            counter <-- metric
            counter.values should equal (Array(1,0,0))

            And("the row is unchanged")
            metric.subject should equal (makeEntry(true, false, true))
        }
        "work if there are no optional objects" in {
            Given("A row with no optional objects")
            val metric = PresentMetric(makeEntry)
            println(metric.subject)
            val counter = ObjectCounter(schemaNonoptional)

            Then("nothing has changed")
            counter <-- metric
            counter.values should have length 0
            metric.subject should equal (makeEntry)
        }
    }
}
