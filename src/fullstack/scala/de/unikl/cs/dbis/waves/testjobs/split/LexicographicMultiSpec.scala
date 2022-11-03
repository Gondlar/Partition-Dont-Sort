package de.unikl.cs.dbis.waves.testjobs.split

import org.scalatest.Inspectors._
import de.unikl.cs.dbis.waves.WavesSpec
import de.unikl.cs.dbis.waves.SparkFixture
import de.unikl.cs.dbis.waves.PartitionTreeMatchers
import de.unikl.cs.dbis.waves.testjobs.IntegrationFixture

import de.unikl.cs.dbis.waves.partitions.{PartitionTree, TreeNode, Spill, SplitByPresence, Bucket}
import de.unikl.cs.dbis.waves.partitions.visitors.operations._
import de.unikl.cs.dbis.waves.partitions.PartitionTreeHDFSInterface
import de.unikl.cs.dbis.waves.sort.LexicographicSorter
import de.unikl.cs.dbis.waves.WavesTable._

import java.io.File

class LexicographicMultiSpec extends WavesSpec
  with IntegrationFixture
  with PartitionTreeMatchers {

  "The LexicographicMulti Split job" should {
    "format the data correctly" in {
      When("we run the manual job")
      LexicographicMulti.main(args)

      Then("We can read the schema")
      val spark = SparkFixture.startSpark()
      val read = PartitionTreeHDFSInterface.apply(spark, wavesPath).read()
      val input = spark.read.json(inputPath)

      val buckets = (0 until 8).map(_ => Bucket())
      val manualShape = buckets.tail.foldLeft(buckets.head: TreeNode.AnyNode[String])((partitioned, spill) => Spill(partitioned, spill))

      read should not be empty
      val schema = read.get
      val tree = new PartitionTree(input.schema, LexicographicSorter, manualShape)
      schema should haveTheSameStructureAs(tree)

      And("The partitions should contain exactly one parquet file")
      assertCleanedPartitions(schema.buckets)

      And("we read the same number of results")
      assertReadableResults(spark)

      And("the log contains what happened")
      val (events, data) = assertLogProperties()
      events should contain theSameElementsInOrderAs (Seq("'split-start'", "'split-done'", "'split-cleanup-end'"))
    }
  }
}