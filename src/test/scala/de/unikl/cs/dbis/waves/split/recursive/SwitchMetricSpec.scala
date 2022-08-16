package de.unikl.cs.dbis.waves.split.recursive

import de.unikl.cs.dbis.waves.WavesSpec

import de.unikl.cs.dbis.waves.Schema
import org.apache.spark.sql.types.{StructType, StructField, IntegerType}

class SwitchMetricSpec extends WavesSpec {
    
    "The SwitchMetric" should {
        "measure if the values of two ObjectCounters are different" in {
            Given("Two 0-1 ObjectCounters")
            val current = new ObjectCounter(Array(0,1,1,0))
            val previous = new ObjectCounter(Array(0,1,0,1))
            val result = ObjectCounter(4)
            val metric = SwitchMetric(current, previous)

            Then("all counts are correct")
            result <-- metric
            result.values should equal (Array(0,0,1,1))

            And("the counters are unchanged")
            current.values should equal (Array(0,1,1,0))
            previous.values should equal (Array(0,1,0,1))
        }
        "handle aliasing on the left side" in {
            Given("Two 0-1 ObjectCounters")
            val current = new ObjectCounter(Array(0,1,1,0))
            val previous = new ObjectCounter(Array(0,1,0,1))
            val metric = SwitchMetric(current, previous)

            Then("all counts are correct")
            current <-- metric
            current.values should equal (Array(0,0,1,1))

            And("the other counter is unchanged")
            previous.values should equal (Array(0,1,0,1))
        }
        "handle aliasing on the right side" in {
            Given("Two 0-1 ObjectCounters")
            val current = new ObjectCounter(Array(0,1,1,0))
            val previous = new ObjectCounter(Array(0,1,0,1))
            val metric = SwitchMetric(current, previous)

            Then("all counts are correct")
            previous <-- metric
            previous.values should equal (Array(0,0,1,1))

            And("the other counter is unchanged")
            current.values should equal (Array(0,1,1,0))
        }
        "work if the counters are empty" in {
            Given("two empty counters")
            val current = ObjectCounter(0)
            val previous = ObjectCounter(0)
            val metric = SwitchMetric(current, previous)
            val result = ObjectCounter(0)

            Then("nothing has changed")
            result <-- metric
            current.values should have length 0
            previous.values should have length 0
            result.values should have length 0
        }
    }
}
