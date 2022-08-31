package de.unikl.cs.dbis.waves.partitions

import de.unikl.cs.dbis.waves.WavesSpec
import de.unikl.cs.dbis.waves.PartitionTrees
import org.apache.spark.sql.sources.IsNull

class MapVisitorSpec extends WavesSpec
  with PartitionTrees {

  "A MapVisitor" should {
    "transform buckets correctly" in {
      val visitor = new MapVisitor[String, Int]({(payload, index) =>
        index should equal (0)
        5
      })
      bucket.accept(visitor)
      visitor.getBucketCount should equal (1)
      visitor.getResult should equal (Bucket(5))
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
      visitor.getResult should equal (SplitByPresence(split.key, Bucket(4), Bucket(4)))
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
      visitor.getResult should equal (Spill(SplitByPresence(split.key, Bucket(4), Bucket(4)), Bucket(4)))
    }
    "visit buckets in the same order as CollectBucketsVisitor" in {
      Given("a map visitor that puts indexes as the payoad and a collect visitor")
      val mapVisitor = new MapVisitor[String, Int]({(payload, index) => index})
      val collectVisitor = new CollectBucketsVisitor[Int]

      When("the collect visitor visits the result of the map visitor")
      spill.accept(mapVisitor)
      mapVisitor.getResult.accept(collectVisitor)

      Then("the data found by the collect visitor is sorted")
      collectVisitor.iter.map{_.data}.toSeq shouldBe (sorted)
    }
  }
}