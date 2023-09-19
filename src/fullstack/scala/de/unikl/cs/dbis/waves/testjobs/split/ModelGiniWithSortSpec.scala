package de.unikl.cs.dbis.waves.testjobs.split

import org.scalatest.Inspectors._
import de.unikl.cs.dbis.waves.WavesSpec

import de.unikl.cs.dbis.waves.partitions.PartitionTree
import de.unikl.cs.dbis.waves.partitions.visitors.operations._
import de.unikl.cs.dbis.waves.sort.NoSorter

import org.apache.spark.sql.types.StructType

class ModelGiniWithSortSpec extends WavesSpec
  with SplitFixture {

  "The ModelGiniWithSort Split job" when {
    "not using schema modifications" should {
      behave like split({
        ModelGiniWithSort.main(args)
      }, specificTests(false))
    }
    "using schema modifications" should {
      behave like split({
        ModelGiniWithSort.main(args :+ "modifySchema=true")
      }, specificTests(true), usesSchemaModifications = true)
    }
  }

  def specificTests(
      modifySchema: Boolean
    )(
    inputSchema: StructType,
    partitionSchema: PartitionTree[String],
    events: Seq[String],
    data: Seq[String]
  ) = {
    And("the partition schema has the correct shape")
    // The order is non-deterministic if multiple splits have the same gini score
    partitionSchema.buckets should have length (8)

    And("the correct sorter is used")
    partitionSchema.sorter should equal (NoSorter)

    And("the log contains what happened")
    events should contain theSameElementsInOrderAs (Seq(
      "'split-start'",
      "'start-CalculateRSIGraph'", "'end-CalculateRSIGraph'",
      "'start-ModelGini'", "'end-ModelGini'",
      "'start-ShuffleByShape'", "'end-ShuffleByShape'",
      "'start-GlobalOrder'", "'end-GlobalOrder'",
      "'start-PriorityStep'", "'step-chosen'", "'end-PriorityStep'",
    ) ++ (if (!modifySchema) Seq.empty else Seq(
      "'start-SchemaModifier'", "'end-SchemaModifier'",
      "'start-Finalizer'", "'end-Finalizer'",
    )) ++ Seq(
      "'start-PrioritySink'", "'writer-chosen'", "'end-PrioritySink'",
      "'split-done'",
      "'split-cleanup-end'"
    ))
  }
}