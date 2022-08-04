package de.unikl.cs.dbis.waves.partitions

import de.unikl.cs.dbis.waves.WavesSpec
import de.unikl.cs.dbis.waves.PartitionTrees

class FindByPathVisitorSpec extends WavesSpec
    with PartitionTrees {

    "A FindByPathVisitor" when {
        "visiting a Bucket" should {
            "find that bucket for an empty path" in {
                val visitor = new FindByPathVisitor(Seq.empty)
                bucket.accept(visitor)
                visitor.result should equal (Some(bucket))
            }
            "find nothing for non-empty path" in {
                val visitor = new FindByPathVisitor(Seq(Absent))
                bucket.accept(visitor)
                visitor.result should equal (None)
            }
        }
        "visiting a split" should {
            "find its absent child for an Absent path" in {
                val visitor = new FindByPathVisitor(Seq(Absent))
                split.accept(visitor)
                visitor.result should equal (Some(split.absentKey))
            }
            "find its present child for an Present path" in {
                val visitor = new FindByPathVisitor(Seq(Present))
                split.accept(visitor)
                visitor.result should equal (Some(split.presentKey))
            }
            "find nothing for other paths" in {
                val visitor = new FindByPathVisitor(Seq(Rest))
                split.accept(visitor)
                visitor.result should equal (None)
            }
        }
        "visiting a spill" should {
            "find the spill's partitioned subtree for a Partitioned path" in {
                val visitor = new FindByPathVisitor(Seq(Partitioned))
                spill.accept(visitor)
                visitor.result should equal (Some(spill.partitioned))
            }
            "find the spill's bucket for a Rest path" in {
                val visitor = new FindByPathVisitor(Seq(Rest))
                spill.accept(visitor)
                visitor.result should equal (Some(spill.rest))
            }
            "find nothing for other paths" in {
                val visitor = new FindByPathVisitor(Seq(Present))
                spill.accept(visitor)
                visitor.result should equal (None)
            }
        }
    }
}