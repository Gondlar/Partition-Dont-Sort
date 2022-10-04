package de.unikl.cs.dbis.waves.partitions.visitors

import de.unikl.cs.dbis.waves.partitions._

import de.unikl.cs.dbis.waves.WavesSpec
import de.unikl.cs.dbis.waves.PartitionTreeFixture

class CollectBucketsVisitorSpec extends WavesSpec
    with PartitionTreeFixture {

    "A CollectBucketsVisitor" when {
        "visiting a Bucket" should {
            "find that bucket" in {
                val visitor = new CollectBucketsVisitor[String]()
                bucket.accept(visitor)
                visitor.result should equal (Seq(bucket))
            }
        }
        "visiting a split" should {
            "find the split's children" in {
                val visitor = new CollectBucketsVisitor[String]()
                split.accept(visitor)
                visitor.result should contain theSameElementsAs (Seq(split.absentKey, split.presentKey))
            }
        }
        "visiting a spill" should {
            "find the spill's children" in {
                val leafs = Seq( spill.rest
                               , spill.partitioned.asInstanceOf[SplitByPresence[String]].presentKey
                               , spill.partitioned.asInstanceOf[SplitByPresence[String]].absentKey
                               )
                val visitor = new CollectBucketsVisitor[String]()
                spill.accept(visitor)
                visitor.result should contain theSameElementsAs (leafs)
            }
        }
    }
}