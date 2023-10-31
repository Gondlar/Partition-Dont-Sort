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
                visitor.result should contain theSameElementsInOrderAs (Seq(split.absentKey, split.presentKey))
            }
        }
        "visiting a splitby value" should {
           "find the split's children" in {
                val visitor = new CollectBucketsVisitor[String]()
                medianOnly.accept(visitor)
                visitor.result should contain theSameElementsInOrderAs (Seq(medianOnly.less, medianOnly.more))
            }
        }
        "visiting a spill" should {
            "find the spill's children" in {
                val leafs = Seq( spill.rest
                               , spill.partitioned.asInstanceOf[SplitByPresence[String]].absentKey
                               , spill.partitioned.asInstanceOf[SplitByPresence[String]].presentKey
                               )
                val visitor = new CollectBucketsVisitor[String]()
                spill.accept(visitor)
                visitor.result should contain theSameElementsInOrderAs (leafs)
            }
        }
        "visiting an n-way split" should {
          "find the n-way split's children" in {
            val leafs = nway.children
            val visitor = new CollectBucketsVisitor[String]()
            nway.accept(visitor)
            visitor.result should contain theSameElementsInOrderAs (leafs)
          }
        }
    }
}