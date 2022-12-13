package de.unikl.cs.dbis.waves.testjobs.split

import org.scalatest.Inspectors._
import de.unikl.cs.dbis.waves.WavesSpec

import de.unikl.cs.dbis.waves.partitions.PartitionTree
import de.unikl.cs.dbis.waves.sort.NoSorter

import org.apache.spark.sql.types.StructType

class RecursiveSwitchSpec extends WavesSpec
  with SplitFixture {

  "The RecursiveSwitch Split job" when {
    "not using schema modifications" should {
      behave like split({
        RecursiveSwitch.main(args)
      }, specificTests)
    }
    "using schema modifications" should {
      behave like split({
        RecursiveSwitch.main(args :+ "modifySchema=true")
      }, specificTests)
    }
  }

  def specificTests(
    inputSchema: StructType,
    partitionSchema: PartitionTree[String],
    events: Seq[String],
    data: Seq[String]
  ) = {
    // The schema is not deterministic if two columns have the same score so
    // we can't test a specific structure

    And("the correct sorter is used")
    partitionSchema.sorter should equal (NoSorter)

    And("the log contains what happened")
    events should contain allOf ("'split-start'", "'split-done'", "'split-cleanup-end'")
  }
}