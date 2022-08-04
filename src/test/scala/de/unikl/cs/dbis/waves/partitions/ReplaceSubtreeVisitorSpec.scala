package de.unikl.cs.dbis.waves.partitions

import de.unikl.cs.dbis.waves.WavesSpec
import de.unikl.cs.dbis.waves.PartitionTrees

class ReplaceSubtreeVisitorSpec extends WavesSpec
    with PartitionTrees {

    "A ReplaceSubtreeVisitor" when {
        "visiting a Bucket" should {
            "be able to replace that bucket" in {
                val replacement = Bucket("replaced")
                val visitor = new ReplaceSubtreeVisitor(bucket, replacement)
                bucket.accept(visitor)
                visitor.getResult should equal (replacement)
            }
            "produce an error if no replacement happened" in {
                val replacement = Bucket("replaced")
                val visitor = new ReplaceSubtreeVisitor(Bucket("foo"), replacement)
                bucket.accept(visitor)
                an [ImpossibleReplacementException] should be thrownBy visitor.getResult
            }
        }
        "visiting a split" should {
            "be able to replace that split" in {
                val replacement = Bucket("replaced")
                val visitor = new ReplaceSubtreeVisitor(split, replacement)
                split.accept(visitor)
                visitor.getResult should equal (replacement)
            }
            "be able to replace that splits child" in {
                val replacement = Bucket("replaced")
                val visitor = new ReplaceSubtreeVisitor(split.absentKey, replacement)
                split.accept(visitor)
                visitor.getResult should equal (SplitByPresence(split.key, split.presentKey, replacement))
            }
            "produce an error if no replacement happened" in {
                val replacement = Bucket("replaced")
                val visitor = new ReplaceSubtreeVisitor(Bucket("foo"), replacement)
                split.accept(visitor)
                an [ImpossibleReplacementException] should be thrownBy visitor.getResult
            }
        }
        "visiting a spill" should {
            "be able to replace that spill" in {
                val replacement = Bucket("replaced")
                val visitor = new ReplaceSubtreeVisitor(spill, replacement)
                spill.accept(visitor)
                visitor.getResult should equal (replacement)
            }
            "be able to replace the spill's bucket with a bucket" in {
                val replacement = Bucket("replaced")
                val visitor = new ReplaceSubtreeVisitor(spill.rest, replacement)
                spill.accept(visitor)
                visitor.getResult should equal (Spill(spill.partitioned,replacement))
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
                visitor.getResult should equal (Spill(replacement, spill.rest))
            }
            "produce an error if no replacement happened" in {
                val replacement = Bucket("replaced")
                val visitor = new ReplaceSubtreeVisitor(Bucket("foo"), replacement)
                spill.accept(visitor)
                an [ImpossibleReplacementException] should be thrownBy visitor.getResult
            }
        }
    }
}