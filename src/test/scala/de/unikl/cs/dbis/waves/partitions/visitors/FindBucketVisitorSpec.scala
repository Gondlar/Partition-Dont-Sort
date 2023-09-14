package de.unikl.cs.dbis.waves.partitions.visitors

import de.unikl.cs.dbis.waves.partitions._

import de.unikl.cs.dbis.waves.WavesSpec
import de.unikl.cs.dbis.waves.PartitionTreeFixture

class FindBucketVisitorSpec extends WavesSpec
    with PartitionTreeFixture {

    "A FindBucketVisitor" when {
        "visiting a Bucket" should {
            "find that bucket" in {
                val visitor = new FindBucketVisitor[String](data(1), schema)
                bucket.accept(visitor)
                visitor.result should equal (bucket)
            }
        }
        "visiting a split" should {
            "find its absent child if the path is absent in the row" in {
                val visitor = new FindBucketVisitor[String](data(1), schema)
                split.accept(visitor)
                visitor.result should equal (split.absentKey)
            }
            "find its present child if the path is present in the row" in {
                val visitor = new FindBucketVisitor[String](data(0), schema)
                split.accept(visitor)
                visitor.result should equal (split.presentKey)
            }
        }
        "visiting a split by value" should {
            "find the less child if the value is less" in {
                val visitor = new FindBucketVisitor[String](data(0), schema)
                val tree = SplitByValue(10, "a", "less", "more")
                tree.accept(visitor)
                visitor.result should equal (tree.less)
            }
            "find the less child if the value is equal" in {
                val visitor = new FindBucketVisitor[String](data(0), schema)
                val tree = SplitByValue(5, "a", "less", "more")
                tree.accept(visitor)
                visitor.result should equal (tree.less)
            }
            "find the more child if the value is more" in {
                val visitor = new FindBucketVisitor[String](data(0), schema)
                val tree = SplitByValue(3, "a", "less", "more")
                tree.accept(visitor)
                visitor.result should equal (tree.more)
            }
            "find the more child if the value is absent" in {
                val visitor = new FindBucketVisitor[String](data(1), schema)
                val tree = SplitByValue(3, "b.d", "less", "more")
                tree.accept(visitor)
                visitor.result should equal (tree.more)
            }
            "find the more child if an ancestor of the value is absent" in {
                val visitor = new FindBucketVisitor[String](data(3), schema)
                val tree = SplitByValue(3, "b.d", "less", "more")
                tree.accept(visitor)
                visitor.result should equal (tree.more)
            }
        }
        "visiting a spill" should {
            "find a bucket from the partitioned subtree" in {
                val visitor = new FindBucketVisitor[String](data(1), schema)
                spill.accept(visitor)
                visitor.result should equal (spill.partitioned.asInstanceOf[SplitByPresence[String]].absentKey)
            }
        }
    }
}