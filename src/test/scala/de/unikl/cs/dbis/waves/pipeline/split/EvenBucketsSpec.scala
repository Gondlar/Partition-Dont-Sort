package de.unikl.cs.dbis.waves.pipeline.split

import de.unikl.cs.dbis.waves.WavesSpec
import de.unikl.cs.dbis.waves.DataFrameFixture

import de.unikl.cs.dbis.waves.partitions.visitors.operations._
import de.unikl.cs.dbis.waves.pipeline._

class EvenBucketsSpec extends WavesSpec
  with DataFrameFixture with PipelineStateFixture {

  "The EvenBuckets Step" should {
    "fail to be constructed" when {
      "zero buckets are requested" in {
        an [IllegalArgumentException] shouldBe thrownBy (EvenBuckets(0))
      }
      "a negative number of buckets is requested" in {
        an [IllegalArgumentException] shouldBe thrownBy (EvenBuckets(-42))
      }
    }
    "always be supported" in {
      (EvenBuckets(2) supports dummyState) shouldBe (true)
    }
    "split a DataFrame evenly" in {
      When("we run the EvenBuckets step")
      val result = EvenBuckets(2)(dummyDfState)

      Then("we see the correct buckets in the result")
      (Buckets isDefinedIn result) shouldBe (true)
      val buckets = Buckets(result)
      buckets should have length (2)
      buckets(0).count should equal (4)
      buckets(1).count should equal (4)

      (NumBuckets isDefinedIn result) shouldBe (true)
      NumBuckets(result) should equal (2)

      And("no shape is set")
      (Shape isDefinedIn result) shouldBe (false)
    }
  }
}