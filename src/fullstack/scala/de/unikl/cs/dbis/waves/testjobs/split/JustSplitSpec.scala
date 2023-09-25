package de.unikl.cs.dbis.waves.testjobs.split

import org.scalatest.Inspectors._
import de.unikl.cs.dbis.waves.WavesSpec
import de.unikl.cs.dbis.waves.PartitionTreeMatchers

import de.unikl.cs.dbis.waves.partitions.{PartitionTree, TreeNode, Spill, Bucket}
import de.unikl.cs.dbis.waves.sort.NoSorter

import org.apache.spark.sql.types.StructType

class JustSplitSpec extends WavesSpec
  with SplitFixture
  with PartitionTreeMatchers {

  "The JustSplit job" should {
    behave like split({
      JustSplit.main(args)
    }, specificTests)
  }

  def specificTests(
    inputSchema: StructType,
    partitionSchema: PartitionTree[String],
    events: Seq[String],
    data: Seq[String]
  ) = {
    And("the correct sorter is used")
    val buckets = (0 to 7).map(_ => Bucket())
    val manualShape = buckets.tail.foldLeft(buckets.head: TreeNode.AnyNode[String])((partitioned, spill) => Spill(partitioned, spill))
    val tree = new PartitionTree(inputSchema, NoSorter, manualShape)
    partitionSchema should haveTheSameStructureAs(tree)

    And("the log contains what happened")
    events should contain theSameElementsInOrderAs (Seq(
      "'split-start'",
      "'start-ParallelEvenBuckets'", "'end-ParallelEvenBuckets'",
      "'start-FlatShapeBuilder'", "'end-FlatShapeBuilder'",
      "'start-ParallelSink'", "'end-ParallelSink'",
      "'split-done'",
      "'split-cleanup-end'"
    ))
  }
}