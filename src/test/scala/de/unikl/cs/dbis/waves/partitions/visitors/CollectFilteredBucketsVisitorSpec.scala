package de.unikl.cs.dbis.waves.partitions.visitors

import de.unikl.cs.dbis.waves.partitions._

import de.unikl.cs.dbis.waves.WavesSpec
import de.unikl.cs.dbis.waves.PartitionTreeFixture
import org.apache.spark.sql.sources.IsNull

class CollectFilteredBucketsVisitorSpec extends WavesSpec
    with PartitionTreeFixture {

    "A CollectFilteredBucketsVisitor" when {
        "visiting a Bucket" should {
            "find that bucket without filters" in {
                val visitor = new CollectFilteredBucketsVisitor[String](Seq.empty)
                bucket.accept(visitor)
                visitor.result should equal (Seq(bucket))

                And("find the same buckets as a CollectBucketsVisitor")
                val visitor2 = new CollectBucketsVisitor[String]()
                bucket.accept(visitor2)
                visitor.result should contain theSameElementsAs (visitor2.result)
            }
            "find that bucket with filters" in {
                val visitor = new CollectFilteredBucketsVisitor[String](Seq(IsNull("b.d")))
                bucket.accept(visitor)
                visitor.result should equal (Seq(bucket))
            }
            "find that bucket with unrelated filters" in {
                val visitor = new CollectFilteredBucketsVisitor[String](Seq(IsNull("bar.foo")))
                bucket.accept(visitor)
                visitor.result should equal (Seq(bucket))
            }
        }
        "visiting a split" should {
            "find the split's children without filters" in {
                val visitor = new CollectFilteredBucketsVisitor[String](Seq.empty)
                split.accept(visitor)
                visitor.result should contain theSameElementsAs (Seq(split.absentKey, split.presentKey))

                And("find the same buckets as a CollectBucketsVisitor")
                val visitor2 = new CollectBucketsVisitor[String]()
                split.accept(visitor2)
                visitor.result should contain theSameElementsAs (visitor2.result)
            }
            "find the split's child with filters" in {
                val visitor = new CollectFilteredBucketsVisitor[String](Seq(IsNull("b.d")))
                split.accept(visitor)
                visitor.result should contain theSameElementsAs (Seq(split.absentKey))
            }
            "find the split's children with unrelated filters" in {
                val visitor = new CollectFilteredBucketsVisitor[String](Seq(IsNull("bar.foo")))
                split.accept(visitor)
                visitor.result should contain theSameElementsAs (Seq(split.absentKey, split.presentKey))
            }
        }
        "visiting a split by value" should {
            "find the split's children without filters" in {
                val visitor = new CollectFilteredBucketsVisitor[String](Seq.empty)
                medianOnly.accept(visitor)
                visitor.result should contain theSameElementsAs (Seq(medianOnly.less, medianOnly.more))

                And("find the same buckets as a CollectBucketsVisitor")
                val visitor2 = new CollectBucketsVisitor[String]()
                medianOnly.accept(visitor2)
                visitor.result should contain theSameElementsAs (visitor2.result)
            }
            "find the split's child with filters" in {
                val visitor = new CollectFilteredBucketsVisitor[String](Seq(IsNull(medianOnly.key.toString())))
                medianOnly.accept(visitor)
                visitor.result should contain theSameElementsAs (Seq(medianOnly.more))
            }
            "find the split's children with unrelated filters" in {
                val visitor = new CollectFilteredBucketsVisitor[String](Seq(IsNull("bar.foo")))
                medianOnly.accept(visitor)
                visitor.result should contain theSameElementsAs (Seq(medianOnly.less, medianOnly.more))
            }
        }
        "visiting a spill" should {
            "find the spill's children without filters" in {
                val leafs = Seq( spill.rest
                                  , spill.partitioned.asInstanceOf[SplitByPresence[String]].presentKey
                                  , spill.partitioned.asInstanceOf[SplitByPresence[String]].absentKey
                                  )
                val visitor = new CollectFilteredBucketsVisitor[String](Seq.empty)
                spill.accept(visitor)
                visitor.result should contain theSameElementsAs (leafs)

                And("find the same buckets as a CollectBucketsVisitor")
                val visitor2 = new CollectBucketsVisitor[String]()
                spill.accept(visitor2)
                visitor.result should contain theSameElementsAs (visitor2.result)
            }
            "find the spill's child with filters" in {
                val leafs = Seq( spill.rest
                                  , spill.partitioned.asInstanceOf[SplitByPresence[String]].absentKey
                                  )
                val visitor = new CollectFilteredBucketsVisitor[String](Seq(IsNull("b.d")))
                spill.accept(visitor)
                visitor.result should contain theSameElementsAs (leafs)
            }
            "find the split's children with unrelated filters" in {
                val leafs = Seq( spill.rest
                                  , spill.partitioned.asInstanceOf[SplitByPresence[String]].presentKey
                                  , spill.partitioned.asInstanceOf[SplitByPresence[String]].absentKey
                                  )
                val visitor = new CollectFilteredBucketsVisitor[String](Seq(IsNull("bar.foo")))
                spill.accept(visitor)
                visitor.result should contain theSameElementsAs (leafs)
            }
        }
        "visiting an n-way split" should {
          "find the n-way split's children without filters" in {
            val leafs = nway.children
            val visitor = new CollectFilteredBucketsVisitor[String](Seq.empty)
            nway.accept(visitor)
            visitor.result should contain theSameElementsAs (leafs)

            And("find the same buckets as a CollectBucketsVisitor")
            val visitor2 = new CollectBucketsVisitor[String]()
            nway.accept(visitor2)
            visitor.result should contain theSameElementsAs (visitor2.result)
          }
          "find the nway split's children with unrelated filters" in {
            val leafs = nway.children
            val visitor = new CollectFilteredBucketsVisitor[String](Seq(IsNull("bar.foo")))
            nway.accept(visitor)
            visitor.result should contain theSameElementsAs (leafs)
          }
        }
    }
}