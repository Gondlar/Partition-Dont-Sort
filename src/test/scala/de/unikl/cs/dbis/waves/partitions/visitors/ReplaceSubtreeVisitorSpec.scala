package de.unikl.cs.dbis.waves.partitions.visitors

import de.unikl.cs.dbis.waves.partitions._

import de.unikl.cs.dbis.waves.WavesSpec
import de.unikl.cs.dbis.waves.PartitionTreeFixture

class ReplaceSubtreeVisitorSpec extends WavesSpec
    with PartitionTreeFixture {

    "A ReplaceSubtreeVisitor" when {
        "visiting a Bucket" should {
            "be able to replace that bucket" in {
                val replacement = Bucket("replaced")
                val visitor = new ReplaceSubtreeVisitor(bucket, replacement)
                bucket.accept(visitor)
                visitor.result should equal (replacement)
            }
            "produce an error if no replacement happened" in {
                val replacement = Bucket("replaced")
                val visitor = new ReplaceSubtreeVisitor(Bucket("foo"), replacement)
                bucket.accept(visitor)
                an [ImpossibleReplacementException] should be thrownBy visitor.result
            }
        }
        "visiting a split" should {
            "be able to replace that split" in {
                val replacement = Bucket("replaced")
                val visitor = new ReplaceSubtreeVisitor(split, replacement)
                split.accept(visitor)
                visitor.result should equal (replacement)
            }
            "be able to replace that splits child" in {
                val replacement = Bucket("replaced")
                val visitor = new ReplaceSubtreeVisitor(split.absentKey, replacement)
                split.accept(visitor)
                visitor.result should equal (SplitByPresence(split.key, split.presentKey, replacement))
            }
            "produce an error if no replacement happened" in {
                val replacement = Bucket("replaced")
                val visitor = new ReplaceSubtreeVisitor(Bucket("foo"), replacement)
                split.accept(visitor)
                an [ImpossibleReplacementException] should be thrownBy visitor.result
            }
        }
        "visiting a split by value" should {
            "be able to replace that split" in {
                val replacement = Bucket("replaced")
                val visitor = new ReplaceSubtreeVisitor(medianOnly, replacement)
                medianOnly.accept(visitor)
                visitor.result should equal (replacement)
            }
            "be able to replace that splits child" in {
                val replacement = Bucket("replaced")
                val visitor = new ReplaceSubtreeVisitor(medianOnly.less, replacement)
                medianOnly.accept(visitor)
                visitor.result should equal (medianOnly.copy(less = replacement))
            }
            "produce an error if no replacement happened" in {
                val replacement = Bucket("replaced")
                val visitor = new ReplaceSubtreeVisitor(Bucket("1234556"), replacement)
                medianOnly.accept(visitor)
                an [ImpossibleReplacementException] should be thrownBy visitor.result
            }
        }
        "visiting a spill" should {
            "be able to replace that spill" in {
                val replacement = Bucket("replaced")
                val visitor = new ReplaceSubtreeVisitor(spill, replacement)
                spill.accept(visitor)
                visitor.result should equal (replacement)
            }
            "be able to replace the spill's bucket with a bucket" in {
                val replacement = Bucket("replaced")
                val visitor = new ReplaceSubtreeVisitor(spill.rest, replacement)
                spill.accept(visitor)
                visitor.result should equal (Spill(spill.partitioned,replacement))
            }
            "be unable to replace the spill's bucket with a non-bucket" in {
                val replacement = SplitByPresence("foo.bar", "foo", "bar")
                val visitor = new ReplaceSubtreeVisitor(spill.rest, replacement)
                an [ImpossibleReplacementException] should be thrownBy (spill.accept(visitor))
            }
            "be able to replace the spill's tree" in {
                val replacement = Bucket("replaced")
                val visitor = new ReplaceSubtreeVisitor(spill.partitioned, replacement)
                spill.accept(visitor)
                visitor.result should equal (Spill(replacement, spill.rest))
            }
            "produce an error if no replacement happened" in {
                val replacement = Bucket("replaced")
                val visitor = new ReplaceSubtreeVisitor(Bucket("foo"), replacement)
                spill.accept(visitor)
                an [ImpossibleReplacementException] should be thrownBy visitor.result
            }
        }
        "visiting an n-way split" can {
          "replace that split" in {
            val replacement = Bucket("replaced")
            val visitor = new ReplaceSubtreeVisitor(nway, replacement)
            nway.accept(visitor)
            visitor.result should equal (replacement)
          }
          "replace the split's first child" in {
            val replacement = Bucket("replaced")
            val visitor = new ReplaceSubtreeVisitor(nway.children(0), replacement)
            nway.accept(visitor)
            visitor.result should equal (EvenNWay(nway.children.updated(0, replacement)))
          }
          "replace the split's last child" in {
            val replacement = Bucket("replaced")
            val visitor = new ReplaceSubtreeVisitor(nway.children(2), replacement)
            nway.accept(visitor)
            visitor.result should equal (EvenNWay(nway.children.updated(2, replacement)))
          }
          "produce an error if no replacement happened" in {
            val replacement = Bucket("replaced")
              val visitor = new ReplaceSubtreeVisitor(Bucket("foo"), replacement)
              nway.accept(visitor)
              an [ImpossibleReplacementException] should be thrownBy visitor.result
          }
        }
    }
}