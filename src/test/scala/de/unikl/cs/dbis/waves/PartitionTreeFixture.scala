package de.unikl.cs.dbis.waves

import org.scalatest.Suite
import org.scalatest.BeforeAndAfterEach
import de.unikl.cs.dbis.waves.partitions.{
  TreeNode, Bucket, Spill, SplitByPresence, PartitionTree
}
import de.unikl.cs.dbis.waves.sort.NoSorter

trait PartitionTreeFixture extends BeforeAndAfterEach
  with SchemaFixture { this: Suite =>

  var bucket : Bucket[String] = null
  var split : SplitByPresence[String] = null
  var spill : Spill[String] = null

  var bucketTree : PartitionTree[String] = null
  var splitTree : PartitionTree[String] = null
  var spillTree : PartitionTree[String] = null
  
  override protected  def beforeEach(): Unit = {
    super.beforeEach()

    bucket = Bucket("foo")
    split = SplitByPresence("b.d", "bar2", "baz2")
    spill = Spill(split, Bucket("foo3"))

    bucketTree = new PartitionTree(schema, NoSorter, bucket)
    splitTree = new PartitionTree(schema, NoSorter, split)
    spillTree = new PartitionTree(schema, NoSorter, spill)
  }
}
