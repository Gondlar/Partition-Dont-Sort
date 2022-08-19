package de.unikl.cs.dbis.waves

import org.scalatest.Suite
import de.unikl.cs.dbis.waves.partitions.{
  TreeNode, Bucket, Spill, SplitByPresence, PartitionTree
}
import org.scalatest.BeforeAndAfterEach

trait PartitionTrees extends BeforeAndAfterEach
  with Schema { this: Suite =>

  var bucket : Bucket[String] = null
  var split : SplitByPresence[String] = null
  var spill : Spill[String] = null

  var bucketTree : PartitionTree[String] = null
  var splitTree : PartitionTree[String] = null
  var spillTree : PartitionTree[String] = null
  
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
