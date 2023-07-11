package de.unikl.cs.dbis.waves.pipeline.split

import de.unikl.cs.dbis.waves.WavesSpec
import de.unikl.cs.dbis.waves.DataFrameFixture

import de.unikl.cs.dbis.waves.partitions.visitors.operations._
import de.unikl.cs.dbis.waves.pipeline._

class RandomBucketsSpec extends WavesSpec
  with DataFrameFixture {

  "The RandomBuckets step" should {
    "fail to be constructed" when {
      "zero buckets are requested" in {
        an [IllegalArgumentException] shouldBe thrownBy (RandomBuckets(0))
      }
      "a negative number of buckets is requested" in {
        an [IllegalArgumentException] shouldBe thrownBy (RandomBuckets(-42))
      }
    }
    "always be supported" in {
      (RandomBuckets(2)  supports PipelineState(null, null)) shouldBe (true)
    }
    "split a DataFrame evenly" in {
      Given("a state")
      val state = PipelineState(df, null)

      When("we run the EvenBuckets step")
      val result = RandomBuckets(2)(state)

      Then("we see the correct buckets in the result")
      (Buckets isDefinedIn result) shouldBe (true)
      val buckets = Buckets(result)
      buckets.length shouldBe <= (2)
      if (buckets.length < 2)
        alert("Got less than 2 partitions. This can be random chance, but is a bug if it persists")
      buckets.map(_.count()).sum should equal (8)

      And("no shape is set")
      (Shape isDefinedIn result) shouldBe (false)
    }
  }
}
