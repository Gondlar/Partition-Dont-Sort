package de.unikl.cs.dbis.waves.partitions.visitors

import de.unikl.cs.dbis.waves.partitions._

import de.unikl.cs.dbis.waves.WavesSpec
import de.unikl.cs.dbis.waves.PartitionTreeFixture
import org.apache.spark.sql.sources.IsNull

class MapVisitorSpec extends WavesSpec
  with PartitionTreeFixture {

  "A MapVisitor" should {
    "transform buckets correctly" in {
      val visitor = new MapVisitor[String, Int]({(payload, index) =>
        index should equal (0)
        5
      })
      bucket.accept(visitor)
      visitor.getBucketCount should equal (1)
      visitor.result should equal (Bucket(5))
    }
    "transform splits correctly" in {
      var expectedIndex = 0
      val visitor = new MapVisitor[String, Int]({(payload, index) =>
        index should equal (expectedIndex)
        expectedIndex += 1
        payload.length
      })
      split.accept(visitor)
      visitor.getBucketCount should equal (2)
      visitor.result should equal (SplitByPresence(split.key, 4, 4))
    }
    "transform splits by value correctly" in {
      var expectedIndex = 0
      val visitor = new MapVisitor[String, Int]({(payload, index) =>
        index should equal (expectedIndex)
        expectedIndex += 1
        payload.length
      })
      medianOnly.accept(visitor)
      visitor.getBucketCount should equal (2)
      visitor.result should equal (SplitByValue(10, "foobar", 3, 3))
    }
    "transform spills correctly" in {
      var expectedIndex = 0
      val visitor = new MapVisitor[String, Int]({(payload, index) =>
        index should equal (expectedIndex)
        expectedIndex += 1
        payload.length
      })
      spill.accept(visitor)
      visitor.getBucketCount should equal (3)
      visitor.result should equal (Spill(SplitByPresence(split.key, 4, 4), Bucket(4)))
    }
    "transform n-way splits correctly" in {
      var expectedIndex = 0
      val visitor = new MapVisitor[String, Int]({(payload, index) =>
        index should equal (expectedIndex)
        expectedIndex += 1
        payload.length
      })
      nway.accept(visitor)
      visitor.getBucketCount should equal (3)
      visitor.result should equal (
        EvenNWay(IndexedSeq(Bucket(3), Bucket(3), Bucket(3)))
      )
    }
    "visit buckets in the same order as CollectBucketsVisitor" in {
      Given("a map visitor that puts indexes as the payoad and a collect visitor")
      val mapVisitor = new MapVisitor[String, Int]({(payload, index) => index})
      val collectVisitor = new CollectBucketsVisitor[Int]

      When("the collect visitor visits the result of the map visitor")
      spill.accept(mapVisitor)
      mapVisitor.result.accept(collectVisitor)

      Then("the data found by the collect visitor is sorted")
      collectVisitor.result.map{_.data} shouldBe sorted
    }
  }
}