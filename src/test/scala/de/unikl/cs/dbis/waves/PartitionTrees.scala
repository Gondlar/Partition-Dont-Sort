package de.unikl.cs.dbis.waves

import org.scalatest.Suite
import de.unikl.cs.dbis.waves.partitions.{
  TreeNode, Bucket, Spill, SplitByPresence, PartitionTree
}
import org.scalatest.BeforeAndAfterEach

trait PartitionTrees extends BeforeAndAfterEach
  with Schema { this: Suite =>

  var bucket : Bucket = null
  var split : SplitByPresence = null
  var spill : Spill = null

  var bucketTree : PartitionTree = null
  var splitTree : PartitionTree = null
  var spillTree : PartitionTree = null
  
  override def beforeEach(): Unit = {
    super.beforeEach()

    bucket = Bucket("foo")
    split = SplitByPresence("b.d", "bar2", "baz2")
    spill = Spill(SplitByPresence("b.d", "bar2", "baz2"), Bucket("foo3"))

    bucketTree = new PartitionTree(schema, bucket)
    splitTree = new PartitionTree(schema, split)
    spillTree = new PartitionTree(schema, spill)
  }
}
