package de.unikl.cs.dbis.waves

import org.scalatest.Suite
import org.scalatest.BeforeAndAfterEach
import de.unikl.cs.dbis.waves.partitions.{
  TreeNode, Bucket, Spill, SplitByPresence, SplitByValue, PartitionTree
}
import de.unikl.cs.dbis.waves.sort.NoSorter

trait PartitionTreeFixture extends BeforeAndAfterEach
  with SchemaFixture { this: Suite =>

  var bucket : Bucket[String] = null
  var split : SplitByPresence[String] = null
  var spill : Spill[String] = null
  var median : SplitByValue[String] = null
  var medianOnly : SplitByValue[String] = null

  var bucketTree : PartitionTree[String] = null
  var splitTree : PartitionTree[String] = null
  var spillTree : PartitionTree[String] = null
  var medianTree : PartitionTree[String] = null
  
  override protected  def beforeEach(): Unit = {
    super.beforeEach()

    bucket = Bucket("foo")
    split = SplitByPresence("b.d", "bar2", "baz2")
    spill = Spill(split, Bucket("foo3"))
    median = SplitByValue(10, "foobar", bucket, split)
    medianOnly = SplitByValue(10, "foobar", bucket, Bucket("bar"))

    bucketTree = new PartitionTree(schema, NoSorter, bucket)
    splitTree = new PartitionTree(schema, NoSorter, split)
    spillTree = new PartitionTree(schema, NoSorter, spill)
    medianTree = new PartitionTree(schema, NoSorter, median)
  }
}
