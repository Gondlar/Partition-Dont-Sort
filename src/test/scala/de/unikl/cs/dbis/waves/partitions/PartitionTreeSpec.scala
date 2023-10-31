package de.unikl.cs.dbis.waves.partitions

import de.unikl.cs.dbis.waves.WavesSpec

import de.unikl.cs.dbis.waves.util.{PartitionFolder,PathKey}
import de.unikl.cs.dbis.waves.PartitionTreeFixture
import de.unikl.cs.dbis.waves.partitions.visitors.ImpossibleReplacementException
import de.unikl.cs.dbis.waves.partitions.visitors.SingleResultVisitor
import de.unikl.cs.dbis.waves.partitions.visitors.operations._
import de.unikl.cs.dbis.waves.sort.NoSorter

class PartitionTreeSpec extends WavesSpec
    with PartitionTreeFixture {
    "A PartitionTree" when {
        "being created" should {
            "reject null as the root" in {
                an [AssertionError] should be thrownBy (new PartitionTree(schema, NoSorter, null))
            }
        }
        "it is just the root" should {
            "be unchanged by JSON conversion" in {
                When("we turn it into JSON and back again")
                val json = bucketTree.toJson
                val tree2 = PartitionTree.fromJson(json)
                Then("the deserialized version is equal")
                tree2 should equal (bucketTree)
            }
            "insert into the root" in {
                bucketTree.getFastInsertLocation should equal (Some(bucket))
                bucketTree.findOrCreateFastInsertLocation(() => "bar") should equal (bucket)

                And("the tree is unchanged")
                bucketTree.root should equal (bucket)
            }
            "contain the root as its only bucket" in {
                bucketTree.buckets.toStream should equal (Seq(bucket))
                bucketTree.bucketsWith(Seq.empty).toStream should equal (Seq(bucket))
            }
            "find all valid paths" in {
                bucketTree.find(Seq.empty) should equal (Some(bucket))
            }
            "not find non-existing paths" in {
                bucketTree.find(Seq(Present)) should equal (None)
                bucketTree.find(Seq(Absent, Present)) should equal (None)
            }
            "be extendable by replacing" in {
                bucketTree.replace(bucket, split)
                bucketTree.root should equal (split)
            }
            "be extendable by replacing by path" in {
                bucketTree.replace(Seq.empty, split)
                bucketTree.root should equal (split)
            }
            "map correctly" in {
                bucketTree.map({(payload, index) => index}) should equal (new PartitionTree(bucketTree.globalSchema, NoSorter, Bucket(0)))
            }
            "modify correctly" in {
                bucketTree.modify({(payload, index) => payload + "SUFFIX"})
                bucketTree.root should equal (Bucket("fooSUFFIX"))
            }
            "map to indexes" in {
                val result = bucketTree.indexes
                result.root should equal (Bucket(0))
            }
            "contain no paths" in {
                val metadata = bucketTree.metadataFor(Seq.empty)
                metadata should equal (PartitionMetadata())
            }
            "that root should have empty metadata" in {
              val metadata = bucketTree.metadata()
              metadata should contain theSameElementsInOrderAs Seq(PartitionMetadata())
            }
        }
        "it starts with a split" should {
            "be unchanged by JSON conversion" in {
                When("we turn it into JSON and back again")
                val json = splitTree.toJson
                val tree2 = PartitionTree.fromJson(json)
                Then("the deserialized version is equal")
                tree2 should equal (splitTree)
            }
            "insert into an new spill partition" in {
                splitTree.getFastInsertLocation should equal (None)
                splitTree.findOrCreateFastInsertLocation(() => "foo3") should equal (spill.rest)

                And("the tree has changed")
                splitTree.root should equal (spill)
            }
            "contain the two child buckets" in {
                val buckets = Seq(split.absentKey, split.presentKey)
                splitTree.buckets.toStream should contain theSameElementsAs (buckets)
                splitTree.bucketsWith(Seq.empty).toStream should contain theSameElementsAs (buckets)
            }
            "find all valid paths" in {
                splitTree.find(Seq.empty) should equal (Some(split))
                splitTree.find(Seq(Present)) should equal (Some(split.presentKey))
                splitTree.find(Seq(Absent)) should equal (Some(split.absentKey))
            }
            "not find non-existing paths" in {
                splitTree.find(Seq(Rest)) should equal (None)
                splitTree.find(Seq(Absent, Present)) should equal (None)
            }
            "be extendable by replacing" in {
                splitTree.replace(split.absentKey, bucket)
                splitTree.root should equal (SplitByPresence("b.d", "bar2", "foo"))
            }
            "be extendable by replacing by path" in {
                splitTree.replace(Seq(Absent), bucket)
                splitTree.root should equal (SplitByPresence("b.d", "bar2", "foo"))
            }
            "map correctly" in {
                splitTree.map({(payload, index) => index}) should equal (new PartitionTree(splitTree.globalSchema, NoSorter, SplitByPresence(split.key, Bucket(1), Bucket(0))))
            }
            "modify correctly" in {
                splitTree.modify({(payload, index) => payload + "SUFFIX"})
                splitTree.root should equal (SplitByPresence(split.key, "bar2SUFFIX", "baz2SUFFIX"))
            }
            "map to indexes" in {
                val result = splitTree.indexes
                result.root should equal (SplitByPresence(split.key, 1, 0))
            }
            "know that path" in {
                val metadata = splitTree.metadataFor(Seq(Present))
                metadata should equal (PartitionMetadata(Seq(split.key), Seq.empty, Seq(Present)))
            }
            "contain that split in its childrens metadata" in {
              val metadata = splitTree.metadata()
              val absentMetadata = PartitionMetadata(Seq.empty, Seq(split.key), Seq(Absent))
              val presentMetadata = PartitionMetadata(Seq(split.key), Seq.empty, Seq(Present))
              metadata should contain theSameElementsInOrderAs Seq(absentMetadata, presentMetadata)
            }
        }
        "it starts with a split by value" should {
            "be unchanged by JSON conversion" in {
                When("we turn it into JSON and back again")
                val json = medianTree.toJson
                val tree2 = PartitionTree.fromJson(json)
                Then("the deserialized version is equal")
                tree2 should equal (medianTree)
            }
            "insert into an new spill partition" in {
                medianTree.getFastInsertLocation should equal (None)
                medianTree.findOrCreateFastInsertLocation(() => "foo3") should equal (spill.rest)

                And("the tree has changed")
                medianTree.root should equal (Spill(median, spill.rest))
            }
            "contain the child buckets" in {
                val buckets = Seq(median.less, split.absentKey, split.presentKey)
                medianTree.buckets.toStream should contain theSameElementsAs (buckets)
                medianTree.bucketsWith(Seq.empty).toStream should contain theSameElementsAs (buckets)
            }
            "find all valid paths" in {
                medianTree.find(Seq.empty) should equal (Some(median))
                medianTree.find(Seq(Less)) should equal (Some(median.less))
                medianTree.find(Seq(MoreOrNull)) should equal (Some(median.more))
            }
            "not find non-existing paths" in {
                medianTree.find(Seq(Rest)) should equal (None)
                medianTree.find(Seq(Absent, Present)) should equal (None)
            }
            "be extendable by replacing" in {
                medianTree.replace(median.more, bucket)
                medianTree.root should equal (SplitByValue(10, "foobar", median.less, bucket))
            }
            "be extendable by replacing by path" in {
                medianTree.replace(Seq(MoreOrNull), bucket)
                medianTree.root should equal (SplitByValue(10, "foobar", median.less, bucket))
            }
            "map correctly" in {
                medianTree.map({(payload, index) => index}) should equal (new PartitionTree(splitTree.globalSchema, NoSorter, SplitByValue(10, "foobar", Bucket(0), SplitByPresence("b.d", 2, 1))))
            }
            "modify correctly" in {
                medianTree.modify({(payload, index) => payload + "SUFFIX"})
                medianTree.root should equal (SplitByValue(10, "foobar", Bucket("fooSUFFIX"), SplitByPresence("b.d", "bar2SUFFIX", "baz2SUFFIX")))
            }
            "map to indexes" in {
                val result = medianTree.indexes
                result.root should equal (SplitByValue(10, "foobar", Bucket(0), SplitByPresence("b.d", 2, 1)))
            }
            "know that path" in {
                val metadata = medianTree.metadataFor(Seq(Less))
                metadata should equal (PartitionMetadata(Seq(median.key), Seq.empty, Seq(Less)))
            }
            "contain that split in its childrens metadata" in {
              val metadata = medianTree.metadata()
              val lessMetadata = PartitionMetadata(Seq(median.key), Seq.empty, Seq(Less))
              val moreMetadata1 = PartitionMetadata(Seq.empty, Seq(PathKey("b.d")), Seq(MoreOrNull, Absent))
              val moreMetadata2 = PartitionMetadata(Seq(PathKey("b.d")), Seq.empty, Seq(MoreOrNull, Present))
              metadata should contain theSameElementsInOrderAs Seq(lessMetadata, moreMetadata1, moreMetadata2)
            }
        }
        "it starts with a spill" should {
            "be unchanged by JSON conversion" in {
                When("we turn it into JSON and back again")
                val json = spillTree.toJson
                val tree2 = PartitionTree.fromJson(json)
                Then("the deserialized version is equal")
                tree2 should equal (spillTree)
            }
            "insert into the spill bucket" in {
                spillTree.getFastInsertLocation should equal (Some(spill.rest))
                spillTree.findOrCreateFastInsertLocation(() => "asdf") should equal (spill.rest)

                And("the tree has not changed")
                spillTree.root should equal (spill)
            }
            "contain the three child buckets" in {
                val buckets = Seq(spill.rest, spill.partitioned.asInstanceOf[SplitByPresence[String]].absentKey, spill.partitioned.asInstanceOf[SplitByPresence[String]].presentKey)
                spillTree.buckets.toStream should contain theSameElementsAs (buckets)
                spillTree.bucketsWith(Seq.empty).toStream should contain theSameElementsAs (buckets)
            }
            "find the valid paths" in {
                spillTree.find(Seq.empty) should equal (Some(spill))
                spillTree.find(Seq(Rest)) should equal (Some(spill.rest))
                spillTree.find(Seq(Partitioned)) should equal (Some(spill.partitioned))
            }
            "not find non-existing paths" in {
                spillTree.find(Seq(Absent)) should equal (None)
                spillTree.find(Seq(Partitioned, Rest)) should equal (None)
            }
            "be extendable by replacing" in {
                spillTree.replace(spill.partitioned, bucket)
                spillTree.root should equal (Spill(Bucket("foo"), Bucket("foo3")))
            }
            "be extendable by replacing by path" in {
                spillTree.replace(Seq(Partitioned), bucket)
                spillTree.root should equal (Spill(Bucket("foo"), Bucket("foo3")))
            }
            "fail to replace the spill partition with a split node" in {
                an [ImpossibleReplacementException] should be thrownBy spillTree.replace(spill.rest, split)
            }
            "map correctly" in {
                spillTree.map({(payload, index) => index}) should equal (new PartitionTree(spillTree.globalSchema, NoSorter, Spill(SplitByPresence(split.key, Bucket(2), Bucket(1)), Bucket(0))))
            }
            "modify correctly" in {
                spillTree.modify({(payload, index) => payload + "SUFFIX"})
                spillTree.root should equal (Spill(SplitByPresence(split.key, "bar2SUFFIX", "baz2SUFFIX"), Bucket("foo3SUFFIX")))
            }
            "map to indexes" in {
                val result = spillTree.indexes
                result.root should equal (Spill(SplitByPresence(split.key, 2, 1), Bucket(0)))
            }
            "find no paths in the root" in {
                val metadata = spillTree.metadataFor(Seq(Partitioned))
                metadata should equal (PartitionMetadata(Seq.empty, Seq.empty, Seq(Partitioned)))
            }
            "contain that spill in its childrens metadata" in {
              val metadata = spillTree.metadata()
              val restMetadata = PartitionMetadata(Seq.empty, Seq.empty, Seq(Rest))
              val absentMetadata = PartitionMetadata(Seq.empty, Seq(split.key), Seq(Partitioned, Absent))
              val presentMetadata = PartitionMetadata(Seq(split.key), Seq.empty, Seq(Partitioned, Present))
              metadata should contain theSameElementsInOrderAs Seq(restMetadata, absentMetadata, presentMetadata)
            }
            "find the node's shape" in {
              spillTree.shape should equal (Spill(SplitByPresence("b.d", (), ()), Bucket(())))
            }
        }
        "it starts with an n-way split" should {
          "be unchanged by JSON conversion" in {
              When("we turn it into JSON and back again")
              val json = nwayTree.toJson
              val tree2 = PartitionTree.fromJson(json)
              Then("the deserialized version is equal")
              tree2 should equal (nwayTree)
            }
        }
    }
}

case class MockVisitor(override val result: Int) extends SingleResultVisitor[String,Int] {
  var visitBucketCalled = false;
  var visitSplitCalled = false;
  var visitSpillCalled = false;
  var visitNWayCalled = false;
  var visitValueCalled = false
  def anyCalled = visitBucketCalled || visitSpillCalled || visitSplitCalled || visitNWayCalled || visitValueCalled

  override def visit(bucket: Bucket[String]): Unit = visitBucketCalled = true
  override def visit(node: SplitByPresence[String]): Unit = visitSplitCalled = true
  override def visit(node: SplitByValue[String]): Unit = visitValueCalled = true
  override def visit(root: Spill[String]): Unit = visitSpillCalled = true
  override def visit(root: EvenNWay[String]): Unit = visitNWayCalled = true
}