package de.unikl.cs.dbis.waves.testjobs.split

import org.scalatest.Inspectors._
import de.unikl.cs.dbis.waves.WavesSpec
import de.unikl.cs.dbis.waves.SparkFixture
import de.unikl.cs.dbis.waves.PartitionTreeMatchers
import de.unikl.cs.dbis.waves.testjobs.IntegrationFixture

import de.unikl.cs.dbis.waves.partitions.{PartitionTree, SplitByPresence, Bucket}
import de.unikl.cs.dbis.waves.partitions.visitors.operations._
import de.unikl.cs.dbis.waves.partitions.PartitionTreeHDFSInterface
import de.unikl.cs.dbis.waves.sort.NoSorter

import java.io.File
import java.io.FileReader
import java.io.BufferedReader

import scala.collection.JavaConverters._

class RecursiveSwitchSpec extends WavesSpec
  with IntegrationFixture
  with PartitionTreeMatchers {

  "The RecursiveSwitch Split job" should {
    "format the data correctly" in {
      When("we run the manual job")
      RecursiveSwitch.main(args)

      Then("We can read the schema")
      val spark = SparkFixture.startSpark()
      val read = PartitionTreeHDFSInterface.apply(spark, wavesPath).read()
      val input = spark.read.json(inputPath)
      
      // The schema is not deterministif if two caolumns have the same score so
      // we can't test a specific structure
      read should not be empty
      val schema = read.get
      schema should have (
        'globalSchema (input.schema),
        'sorter (NoSorter)
      )

      And("The partitions should contain exactly one parquet file")
      assertCleanedPartitions(schema.buckets)

      And("we read the same results")
      assertReadableResults(spark)

      And("the log contains what happened")
      val (events, data) = assertLogProperties()
      events should contain allOf ("'split-start'", "'split-done'", "'split-cleanup-end'")
    }
  }
}