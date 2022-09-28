package de.unikl.cs.dbis.waves.partitions.visitors

import de.unikl.cs.dbis.waves.partitions._

import de.unikl.cs.dbis.waves.WavesSpec
import de.unikl.cs.dbis.waves.PartitionTreeFixture

class FindBucketVisitorSpec extends WavesSpec
    with PartitionTreeFixture {

    "A FindBucketVisitor" when {
        "visiting a Bucket" should {
            "find that bucket" in {
                val visitor = new FindBucketVisitor[String](internalData(1), schema)
                bucket.accept(visitor)
                visitor.result should equal (bucket)
            }
        }
        "visiting a split" should {
            "find its absent child if the path is absent in the row" in {
                val visitor = new FindBucketVisitor[String](internalData(1), schema)
                split.accept(visitor)
                visitor.result should equal (split.absentKey)
            }
            "find its present child if the path is present in the row" in {
                val visitor = new FindBucketVisitor[String](internalData(0), schema)
                split.accept(visitor)
                visitor.result should equal (split.presentKey)
            }
        }
        "visiting a spill" should {
            "find a bucket from the partitioned subtree" in {
                val visitor = new FindBucketVisitor[String](internalData(1), schema)
                spill.accept(visitor)
                visitor.result should equal (spill.partitioned.asInstanceOf[SplitByPresence[String]].absentKey)
            }
        }
    }
}