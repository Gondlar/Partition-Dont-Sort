package de.unikl.cs.dbis.waves.pipeline.split

import de.unikl.cs.dbis.waves.WavesSpec
import de.unikl.cs.dbis.waves.DataFrameFixture

import de.unikl.cs.dbis.waves.partitions.visitors.operations._
import de.unikl.cs.dbis.waves.pipeline._
import de.unikl.cs.dbis.waves.partitions.Spill
import de.unikl.cs.dbis.waves.partitions.Bucket

class ParallelEvenBucketsSpec extends WavesSpec
  with DataFrameFixture with PipelineStateFixture {

  "The ParallelEvenBuckets Step" should {
    "fail to be constructed" when {
      "zero buckets are requested" in {
        an [IllegalArgumentException] shouldBe thrownBy (ParallelEvenBuckets(0))
      }
      "a negative number of buckets is requested" in {
        an [IllegalArgumentException] shouldBe thrownBy (ParallelEvenBuckets(-42))
      }
    }
    "be supported by the default state" in {
      (ParallelEvenBuckets(2) supports dummyState) shouldBe (true)
    }
    "not be supported when schema modifications are requested" in {
      val state = ModifySchema(dummyState) = true
      (ParallelEvenBuckets(2) supports state) shouldBe (false)
    }
    "split a DataFrame evenly" in {
      When("we run the ParallelEvenBuckets step")
      val result = ParallelEvenBuckets(4)(dummyDfState)

      Then("we see the correct buckets in the result")
      (Shape isDefinedIn result) shouldBe (true)
      val shape = Shape(result)
      shape should equal (Spill(Spill(Spill(Bucket(()),Bucket(())),Bucket(())),Bucket(())))

      (NumBuckets isDefinedIn result) shouldBe (true)
      NumBuckets(result) should equal (4)

      result.data.rdd.getNumPartitions should equal (4)
      result.data.collect() should contain theSameElementsAs (df.collect())
      result.data.rdd.mapPartitions(partition => Iterator(partition.size)).collect() should equal (Array(2,2,2,2))
    }
  }
}