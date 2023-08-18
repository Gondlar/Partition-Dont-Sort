package de.unikl.cs.dbis.waves.partitions

import de.unikl.cs.dbis.waves.WavesSpec

import de.unikl.cs.dbis.waves.util.{PartitionFolder,PathKey}
import de.unikl.cs.dbis.waves.PartitionTreeFixture
import de.unikl.cs.dbis.waves.partitions.visitors.ImpossibleReplacementException
import de.unikl.cs.dbis.waves.partitions.visitors.SingleResultVisitor
import de.unikl.cs.dbis.waves.partitions.visitors.operations._

class TreeNodeSpec extends WavesSpec
  with PartitionTreeFixture {

  val itIsA = afterWord("it is a")

  "A TreeNode" when itIsA {
    "bucket" can {
      "be created using a random name" in {
        Bucket().data shouldNot equal ("")
      }
      "return the correct folder" in {
        bucket.folder("bar") should equal (new PartitionFolder("bar", "foo", false))
      }
      "accept a visitor" in {
        val visitor = MockVisitor(5)
        bucket.accept(visitor)
        visitor.visitBucketCalled shouldBe (true)
        visitor.visitSpillCalled shouldBe (false)
        visitor.visitSplitCalled shouldBe (false)
      }
      "accept a SingleResultVisitor" in {
        bucket(MockVisitor(4)) should equal (4)
      }
    }
    "split" can {
      "be created from a node's string representation" in {
        SplitByPresence("foo.bar", Bucket("abc"), Bucket("cde")) should equal (SplitByPresence(PathKey("foo.bar"), Bucket("abc"), Bucket("cde")))
      }
      "be created from leaf node names" in {
        SplitByPresence("foo.bar", "abc", "cde") should equal (SplitByPresence(PathKey("foo.bar"), Bucket("abc"), Bucket("cde")))
      }
      "be created from leaf node names and a PathKey" in {
        SplitByPresence(PathKey("foo.bar"), "abc", "cde") should equal (SplitByPresence(PathKey("foo.bar"), Bucket("abc"), Bucket("cde")))
      }
      "accept a visitor" in {
        val visitor = MockVisitor(5)
        split.accept(visitor)
        visitor.visitBucketCalled shouldBe (false)
        visitor.visitSpillCalled shouldBe (false)
        visitor.visitSplitCalled shouldBe (true)
      }
      "accept a SingleResultVisitor" in {
        split(MockVisitor(4)) should equal (4)
      }
    }
    "spill" can {
      "accept a visitor" in {
        val visitor = MockVisitor(5)
        spill.accept(visitor)
        visitor.visitBucketCalled shouldBe (false)
        visitor.visitSpillCalled shouldBe (true)
        visitor.visitSplitCalled shouldBe (false)
      }
      "accept a SingleResultVisitor" in {
        spill(MockVisitor(4)) should equal (4)
      }
    }
  }
  it can {
    "be converted to JSON and back" when itIsA {
      import PartitionTree._
      "bucket" in {
        PartitionTree.treeFromJson(bucket.toJson) should equal (bucket)
      }
      "split" in {
        PartitionTree.treeFromJson(split.toJson) should equal (split)
      }
      "spill" in {
        PartitionTree.treeFromJson(spill.toJson) should equal (spill)
      }
    }
    "identify its buckets" when itIsA {
      "bucket" in {
        bucket.buckets should equal (Seq(bucket))
        bucket.bucketsWith(Seq.empty) should equal (Seq(bucket))
      }
      "split" in {
        val buckets = Seq(split.absentKey, split.presentKey)
        split.buckets should contain theSameElementsAs (buckets)
        split.bucketsWith(Seq.empty) should contain theSameElementsAs (buckets)
      }
      "spill" in {
        val buckets = Seq(spill.rest, spill.partitioned.asInstanceOf[SplitByPresence[String]].absentKey, spill.partitioned.asInstanceOf[SplitByPresence[String]].presentKey)
        spill.buckets.toStream should contain theSameElementsAs (buckets)
        spill.bucketsWith(Seq.empty).toStream should contain theSameElementsAs (buckets)
      }
    }
    "find valid paths" when itIsA {
      "bucket" in {
        bucket.find(Seq.empty) should equal (Some(bucket))
      }
      "split" in {
        split.find(Seq.empty) should equal (Some(split))
        split.find(Seq(Present)) should equal (Some(split.presentKey))
        split.find(Seq(Absent)) should equal (Some(split.absentKey))
      }
      "spill" in {
        spill.find(Seq.empty) should equal (Some(spill))
        spill.find(Seq(Rest)) should equal (Some(spill.rest))
        spill.find(Seq(Partitioned)) should equal (Some(spill.partitioned))
      }
    }
    "not find non-existing paths" when itIsA {
      "bucket" in {
        bucket.find(Seq(Present)) should equal (None)
        bucket.find(Seq(Absent, Present)) should equal (None)
      }
      "split" in {
        split.find(Seq(Rest)) should equal (None)
        split.find(Seq(Absent, Present)) should equal (None)
      }
      "spill" in {
        spill.find(Seq(Absent)) should equal (None)
        spill.find(Seq(Partitioned, Rest)) should equal (None)
      }
    }
    "be extended by replacing" when itIsA {
      "bucket" in {
        bucket.replace(bucket, split) should equal (split)
      }
      "split" in {
        split.replace(split.absentKey, bucket) should equal (SplitByPresence("b.d", "bar2", "foo"))
      }
      "spill" in {
        spill.replace(spill.partitioned, bucket) should equal (Spill(Bucket("foo"), Bucket("foo3")))
      }
    }
    "not replace a spill partition with a split node" in {
      an [ImpossibleReplacementException] should be thrownBy spill.replace(spill.rest, split)
    }
    "be extended by replacing by path" when itIsA {
      "bucket" in {
        bucket.replace(Seq.empty, split) should equal (split)
      }
      "split" in {
        split.replace(Seq(Absent), bucket) should equal (SplitByPresence("b.d", "bar2", "foo"))
      }
      "spill" in {
        spill.replace(Seq(Partitioned), bucket) should equal (Spill(Bucket("foo"), Bucket("foo3")))
      }
    }
    "be mapped" when itIsA {
      "bucket" in {
        bucket.map({(payload, index) => index}) should equal (Bucket(0))
      }
      "split" in {
        split.map({(payload, index) => index}) should equal (SplitByPresence(split.key, Bucket(1), Bucket(0)))
      }
      "spill" in {
        spill.map({(payload, index) => index}) should equal (Spill(SplitByPresence(split.key, Bucket(2), Bucket(1)), Bucket(0)))
      }
    }
    "find all its Buckets' metadata" when itIsA {
      "bucket" in {
        bucket.metadata() should contain theSameElementsInOrderAs Seq(PartitionMetadata())
      }
      "split" in {
        val absentMetadata = PartitionMetadata(Seq.empty, Seq(split.key), Seq(Absent))
        val presentMetadata = PartitionMetadata(Seq(split.key), Seq.empty, Seq(Present))
        split.metadata() should contain theSameElementsInOrderAs Seq(absentMetadata, presentMetadata)
      }
      "spill" in {
        val restMetadata = PartitionMetadata(Seq.empty, Seq.empty, Seq(Rest))
        val absentMetadata = PartitionMetadata(Seq.empty, Seq(split.key), Seq(Partitioned, Absent))
        val presentMetadata = PartitionMetadata(Seq(split.key), Seq.empty, Seq(Partitioned, Present))
        spill.metadata() should contain theSameElementsInOrderAs Seq(restMetadata, absentMetadata, presentMetadata)
      }
    }
    "find its metadata by path" when itIsA {
      "bucket" in {
        bucket.metadataFor(Seq.empty) should equal (PartitionMetadata())
      }
      "split" in {
        split.metadataFor(Seq(Present)) should equal (PartitionMetadata(Seq(split.key), Seq.empty, Seq(Present)))
      }
      "spill" in {
        spill.metadataFor(Seq(Partitioned)) should equal (PartitionMetadata(Seq.empty, Seq.empty, Seq(Partitioned)))
      }
    }
  }    
}
