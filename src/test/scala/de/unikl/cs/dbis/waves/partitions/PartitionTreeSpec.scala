package de.unikl.cs.dbis.waves.partitions

import de.unikl.cs.dbis.waves.WavesSpec

import de.unikl.cs.dbis.waves.util.{PartitionFolder,PathKey}
import de.unikl.cs.dbis.waves.PartitionTreeFixture

class PartitionTreeSpec extends WavesSpec
    with PartitionTreeFixture {

    "A TreeNode" when {
        "it is a Bucket" should {
            "return the correct folder" in {
                bucket.folder("bar") should equal (new PartitionFolder("bar", "foo", false))
            }
        }
        "it is a SplitByPresence" should {
            "be createable from a node's string representation" in {
                SplitByPresence("foo.bar", Bucket("abc"), Bucket("cde")) should equal (SplitByPresence(PathKey("foo.bar"), Bucket("abc"), Bucket("cde")))
            }
            "be creatable from leaf node names" in {
                SplitByPresence("foo.bar", "abc", "cde") should equal (SplitByPresence(PathKey("foo.bar"), Bucket("abc"), Bucket("cde")))
            }
        }
    }
    "A PartitionTree" when {
        "being created" should {
            "reject null as the root" in {
                an [AssertionError] should be thrownBy (new PartitionTree(schema, null))
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
                bucketTree.getBuckets().toStream should equal (Seq(bucket))
                bucketTree.getBuckets(Seq.empty).toStream should equal (Seq(bucket))
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
            "map correctly" in {
                bucketTree.map({(payload, index) => index}) should equal (new PartitionTree(bucketTree.globalSchema, Bucket(0)))
            }
            "modify correctly" in {
                bucketTree.modify({(payload, index) => payload + "SUFFIX"})
                bucketTree.root should equal (Bucket("fooSUFFIX"))
            }
            "contain no paths" in {
                val (absent, present) = bucketTree.knownAbsentAndPresentIn(Seq.empty)
                absent shouldBe empty
                present shouldBe empty
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
                splitTree.getBuckets().toStream should contain theSameElementsAs (buckets)
                splitTree.getBuckets(Seq.empty).toStream should contain theSameElementsAs (buckets)
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
            "map correctly" in {
                splitTree.map({(payload, index) => index}) should equal (new PartitionTree(splitTree.globalSchema, SplitByPresence(split.key, Bucket(1), Bucket(0))))
            }
            "modify correctly" in {
                splitTree.modify({(payload, index) => payload + "SUFFIX"})
                splitTree.root should equal (SplitByPresence(split.key, Bucket("bar2SUFFIX"), Bucket("baz2SUFFIX")))
            }
            "know that path" in {
                val (absent, present) = splitTree.knownAbsentAndPresentIn(Seq(Present))
                absent shouldBe empty
                present should contain theSameElementsAs (Seq(split.key))
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
                spillTree.getBuckets().toStream should contain theSameElementsAs (buckets)
                spillTree.getBuckets(Seq.empty).toStream should contain theSameElementsAs (buckets)
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
            "fail to replave the spill partition with a split node" in {
                an [ImpossibleReplacementException] should be thrownBy spillTree.replace(spill.rest, split)
            }
            "map correctly" in {
                spillTree.map({(payload, index) => index}) should equal (new PartitionTree(spillTree.globalSchema, Spill(SplitByPresence(split.key, Bucket(2), Bucket(1)), Bucket(0))))
            }
            "modify correctly" in {
                spillTree.modify({(payload, index) => payload + "SUFFIX"})
                spillTree.root should equal (Spill(SplitByPresence(split.key, Bucket("bar2SUFFIX"), Bucket("baz2SUFFIX")), Bucket("foo3SUFFIX")))
            }
            "find no paths in the root" in {
                val (absent, present) = spillTree.knownAbsentAndPresentIn(Seq(Partitioned))
                absent shouldBe empty
                present shouldBe empty
            }
        }
    }
    
}