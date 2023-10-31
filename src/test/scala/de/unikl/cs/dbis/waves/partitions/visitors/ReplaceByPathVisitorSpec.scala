package de.unikl.cs.dbis.waves.partitions.visitors

import de.unikl.cs.dbis.waves.partitions._

import de.unikl.cs.dbis.waves.WavesSpec
import de.unikl.cs.dbis.waves.PartitionTreeFixture

class ReplaceByPathVisitorSpec extends WavesSpec
    with PartitionTreeFixture {

    def replace = afterWord("replace")
    def fail = afterWord("produce an error")

    "A ReplaceByPathVisitor" when {
      "visiting a Bucket" can replace {
        "it entriely given an empty path" in {
          val visitor = new ReplaceByPathVisitor(Seq.empty, spill)
          bucket.accept(visitor)
          visitor.result should equal (spill)
        }
      }
      "visiting a Bucket" should fail {
        "for invalid paths" in {
          val visitor = new ReplaceByPathVisitor(Seq(Absent), spill)
          an [InvalidPathException] should be thrownBy bucket.accept(visitor)
        }
      }
      "visiting a split" can replace {
        "it entirely given an empty path" in {
          val visitor = new ReplaceByPathVisitor(Seq.empty, bucket)
          split.accept(visitor)
          visitor.result should equal (bucket)
        }
        "the present child" in {
          val replacement = Bucket("replaced")
          val visitor = new ReplaceByPathVisitor(Seq(Present), replacement)
          split.accept(visitor)
          visitor.result should equal (split.copy(presentKey = replacement))
        }
        "the absent child" in {
          val replacement = Bucket("replaced")
          val visitor = new ReplaceByPathVisitor(Seq(Absent), replacement)
          split.accept(visitor)
          visitor.result should equal (split.copy(absentKey = replacement))
        }
      }
      "visiting a split" should fail {
        "for invalid paths" in {
          val visitor = new ReplaceByPathVisitor(Seq(Rest), bucket)
          an [InvalidPathException] should be thrownBy split.accept(visitor)
        }
      }
      "visiting a split by value" can replace {
        "it entirely given an empty path" in {
          val visitor = new ReplaceByPathVisitor(Seq.empty, bucket)
          medianOnly.accept(visitor)
          visitor.result should equal (bucket)
        }
        "the less child" in {
          val replacement = Bucket("replaced")
          val visitor = new ReplaceByPathVisitor(Seq(Less), replacement)
          medianOnly.accept(visitor)
          visitor.result should equal (medianOnly.copy(less = replacement))
        }
        "the more child" in {
          val replacement = Bucket("replaced")
          val visitor = new ReplaceByPathVisitor(Seq(MoreOrNull), replacement)
          medianOnly.accept(visitor)
          visitor.result should equal (medianOnly.copy(more = replacement))
        }
      }
      "visiting a split by value" should fail {
        "for invalid paths" in {
          val visitor = new ReplaceByPathVisitor(Seq(Rest), bucket)
          an [InvalidPathException] should be thrownBy medianOnly.accept(visitor)
        }
      }
      "visiting a spill" can replace {
        "it entirely given an empty path" in {
          val visitor = new ReplaceByPathVisitor(Seq.empty, bucket)
          spill.accept(visitor)
          visitor.result should equal (bucket)
        }
        "the bucket with a bucket" in {
          val replacement = Bucket("replaced")
          val visitor = new ReplaceByPathVisitor(Seq(Rest), replacement)
          spill.accept(visitor)
          visitor.result should equal (spill.copy(rest = replacement))
        }
        "the subtree" in {
          val replacement = Bucket("replaced")
          val visitor = new ReplaceByPathVisitor(Seq(Partitioned), replacement)
          spill.accept(visitor)
          visitor.result should equal (spill.copy(partitioned = replacement))
        }
      }
      "visiting a spill" should fail {
        "for invalid paths" in {
          val visitor = new ReplaceByPathVisitor(Seq(Absent), bucket)
          an [InvalidPathException] should be thrownBy spill.accept(visitor)
        }
        "when replace the spill's bucket with a non-bucket" in {
          val visitor = new ReplaceByPathVisitor(Seq(Rest), split)
          an [ImpossibleReplacementException] should be thrownBy (spill.accept(visitor))
        }
      }
      "visiting an n-way path" can replace {
        "it entirely given an empty path" in {
          val visitor = new ReplaceByPathVisitor(Seq.empty, bucket)
          nway.accept(visitor)
          visitor.result should equal (bucket)
        }
        "the first bucket" in {
          val replacement = Bucket("replaced")
          val visitor = new ReplaceByPathVisitor(Seq(NWayPath(0)), replacement)
          nway.accept(visitor)
          visitor.result should equal (EvenNWay(nway.children.updated(0, replacement)))
        }
        "the last bucket" in {
          val replacement = Bucket("replaced")
          val visitor = new ReplaceByPathVisitor(Seq(NWayPath(2)), replacement)
          nway.accept(visitor)
          visitor.result should equal (EvenNWay(nway.children.updated(2, replacement)))
        }
      }
      "visiting an n-way split" should fail {
        "for invalid paths" in {
          val visitor = new ReplaceByPathVisitor(Seq(Absent), bucket)
          an [InvalidPathException] should be thrownBy nway.accept(visitor)
        }
      }
    }
}