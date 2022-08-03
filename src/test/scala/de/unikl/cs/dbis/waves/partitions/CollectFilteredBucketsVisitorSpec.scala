package de.unikl.cs.dbis.waves.partitions

import de.unikl.cs.dbis.waves.WavesSpec
import de.unikl.cs.dbis.waves.PartitionTrees
import org.apache.spark.sql.sources.IsNull

class CollectFilteredBucketsVisitorSpec extends WavesSpec
    with PartitionTrees {

    "A CollectFilteredBucketsVisitor" when {
        "visiting a Bucket" should {
            "find that bucket without filters" in {
                val visitor = new CollectFilteredBucketsVisitor(Seq.empty)
                bucket.accept(visitor)
                visitor.iter.toStream should equal (Seq(bucket))

                And("find the same buckets as a CollectBucketsVisitor")
                val visitor2 = new CollectBucketsVisitor()
                bucket.accept(visitor2)
                visitor.iter.toStream should contain theSameElementsAs (visitor2.iter.toStream)
            }
            "find that bucket with filters" in {
                val visitor = new CollectFilteredBucketsVisitor(Seq(IsNull("foo.bar")))
                bucket.accept(visitor)
                visitor.iter.toStream should equal (Seq(bucket))
            }
            "find that bucket with unrelated filters" in {
                val visitor = new CollectFilteredBucketsVisitor(Seq(IsNull("bar.foo")))
                bucket.accept(visitor)
                visitor.iter.toStream should equal (Seq(bucket))
            }
        }
        "visiting a split" should {
            "find the split's children without filters" in {
                val visitor = new CollectFilteredBucketsVisitor(Seq.empty)
                split.accept(visitor)
                visitor.iter.toStream should contain theSameElementsAs (Seq(split.absentKey, split.presentKey))

                And("find the same buckets as a CollectBucketsVisitor")
                val visitor2 = new CollectBucketsVisitor()
                split.accept(visitor2)
                visitor.iter.toStream should contain theSameElementsAs (visitor2.iter.toStream)
            }
            "find the split's child with filters" in {
                val visitor = new CollectFilteredBucketsVisitor(Seq(IsNull("foo.bar")))
                split.accept(visitor)
                visitor.iter.toStream should contain theSameElementsAs (Seq(split.absentKey))
            }
            "find the split's children with unrelated filters" in {
                val visitor = new CollectFilteredBucketsVisitor(Seq(IsNull("bar.foo")))
                split.accept(visitor)
                visitor.iter.toStream should contain theSameElementsAs (Seq(split.absentKey, split.presentKey))
            }
        }
        "visiting a spill" should {
            "find the spill's children without filters" in {
                val leafs = Seq( spill.rest
                                  , spill.partitioned.asInstanceOf[SplitByPresence].presentKey
                                  , spill.partitioned.asInstanceOf[SplitByPresence].absentKey
                                  )
                val visitor = new CollectFilteredBucketsVisitor(Seq.empty)
                spill.accept(visitor)
                visitor.iter.toStream should contain theSameElementsAs (leafs)

                And("find the same buckets as a CollectBucketsVisitor")
                val visitor2 = new CollectBucketsVisitor()
                spill.accept(visitor2)
                visitor.iter.toStream should contain theSameElementsAs (visitor2.iter.toStream)
            }
            "find the spill's child with filters" in {
                val leafs = Seq( spill.rest
                                  , spill.partitioned.asInstanceOf[SplitByPresence].absentKey
                                  )
                val visitor = new CollectFilteredBucketsVisitor(Seq(IsNull("foo.bar")))
                spill.accept(visitor)
                visitor.iter.toStream should contain theSameElementsAs (leafs)
            }
            "find the split's children with unrelated filters" in {
                val leafs = Seq( spill.rest
                                  , spill.partitioned.asInstanceOf[SplitByPresence].presentKey
                                  , spill.partitioned.asInstanceOf[SplitByPresence].absentKey
                                  )
                val visitor = new CollectFilteredBucketsVisitor(Seq(IsNull("bar.foo")))
                spill.accept(visitor)
                visitor.iter.toStream should contain theSameElementsAs (leafs)
            }
        }
    }
}