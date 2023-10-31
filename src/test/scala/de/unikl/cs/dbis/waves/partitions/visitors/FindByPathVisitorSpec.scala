package de.unikl.cs.dbis.waves.partitions.visitors

import de.unikl.cs.dbis.waves.partitions._

import de.unikl.cs.dbis.waves.WavesSpec
import de.unikl.cs.dbis.waves.PartitionTreeFixture

class FindByPathVisitorSpec extends WavesSpec
    with PartitionTreeFixture {

    "A FindByPathVisitor" when {
        "visiting a Bucket" should {
            "find that bucket for an empty path" in {
                val visitor = new FindByPathVisitor[String](Seq.empty)
                bucket.accept(visitor)
                visitor.result should equal (Some(bucket))
            }
            "find nothing for non-empty path" in {
                val visitor = new FindByPathVisitor[String](Seq(Absent))
                bucket.accept(visitor)
                visitor.result should equal (None)
            }
        }
        "visiting a split" should {
            "find its absent child for an Absent path" in {
                val visitor = new FindByPathVisitor[String](Seq(Absent))
                split.accept(visitor)
                visitor.result should equal (Some(split.absentKey))
            }
            "find its present child for an Present path" in {
                val visitor = new FindByPathVisitor[String](Seq(Present))
                split.accept(visitor)
                visitor.result should equal (Some(split.presentKey))
            }
            "find nothing for other paths" in {
                val visitor = new FindByPathVisitor[String](Seq(Rest))
                split.accept(visitor)
                visitor.result should equal (None)
            }
        }
        "visiting a split by value" should {
            "find its less child for a Less path" in {
                val visitor = new FindByPathVisitor[String](Seq(Less))
                medianOnly.accept(visitor)
                visitor.result should equal (Some(medianOnly.less))
            }
            "find its more child for an MoreOrAbsent path" in {
                val visitor = new FindByPathVisitor[String](Seq(MoreOrNull))
                medianOnly.accept(visitor)
                visitor.result should equal (Some(medianOnly.more))
            }
            "find nothing for other paths" in {
                val visitor = new FindByPathVisitor[String](Seq(Rest))
                medianOnly.accept(visitor)
                visitor.result should equal (None)
            }
        }
        "visiting a spill" should {
            "find the spill's partitioned subtree for a Partitioned path" in {
                val visitor = new FindByPathVisitor[String](Seq(Partitioned))
                spill.accept(visitor)
                visitor.result should equal (Some(spill.partitioned))
            }
            "find the spill's bucket for a Rest path" in {
                val visitor = new FindByPathVisitor[String](Seq(Rest))
                spill.accept(visitor)
                visitor.result should equal (Some(spill.rest))
            }
            "find nothing for other paths" in {
                val visitor = new FindByPathVisitor[String](Seq(Present))
                spill.accept(visitor)
                visitor.result should equal (None)
            }
        }
        "visiting an n-way split" should {
          "find the first node for an NWay(0) path" in {
            val visitor = new FindByPathVisitor[String](Seq(NWayPath(0)))
            nway.accept(visitor)
            visitor.result.value should equal (nway.children(0))
          }
          "find the last node for an NWay path with the correct index" in {
            val visitor = new FindByPathVisitor[String](Seq(NWayPath(2)))
            nway.accept(visitor)
            visitor.result.value should equal (nway.children(2))
          }
          "find nothing for an NWay path with a too high index" in {
            val visitor = new FindByPathVisitor[String](Seq(NWayPath(3)))
            nway.accept(visitor)
            visitor.result should not be ('defined)
          }
          "not be able to contruct the path for a negative index" in {
            an [IllegalArgumentException] shouldBe thrownBy (NWayPath(-1))
          }
          "find nothing for a different path" in {
            val visitor = new FindByPathVisitor[String](Seq(Absent))
            nway.accept(visitor)
            visitor.result should not be ('defined)
          }
        }
    }
}