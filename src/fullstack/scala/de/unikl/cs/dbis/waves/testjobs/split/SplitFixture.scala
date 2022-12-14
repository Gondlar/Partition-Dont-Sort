package de.unikl.cs.dbis.waves.testjobs.split

import de.unikl.cs.dbis.waves.testjobs.IntegrationFixture

import de.unikl.cs.dbis.waves.SparkFixture
import de.unikl.cs.dbis.waves.partitions.PartitionTreeHDFSInterface
import de.unikl.cs.dbis.waves.partitions.PartitionTree
import de.unikl.cs.dbis.waves.partitions.visitors.operations._

import org.apache.spark.sql.types.StructType

trait SplitFixture extends IntegrationFixture {

  def split(
    splitAction: => Unit,
    testAction: (StructType, PartitionTree[String], Seq[String], Seq[String]) => Unit,
    enforceSingleFilesPerPartition:  Boolean = true,
    usesSchemaModifications: Boolean = false
  ) = {
    "format the data correctly" in {
      When("we run the job")
      splitAction

      Then("we can read the schema")
      val spark = SparkFixture.startSpark()
      val readSchema = PartitionTreeHDFSInterface.apply(spark, wavesPath).read()
      val input = spark.read.json(inputPath)
      
      readSchema should not be empty
      val schema = readSchema.get
      schema.globalSchema should equal (input.schema)

      if (enforceSingleFilesPerPartition) {
        And("the partitions should contain exactly one parquet file")
        assertCleanedPartitions(spark, schema.buckets)
      }

      if (usesSchemaModifications) {
        And("The schema is modified")
        assertModifiedSchema(spark, schema.buckets)
      } else {
        And("The schema is not modified")
        assertUnmodifiedSchema(spark, schema.buckets)
      }

      And("we read the same results")
      assertReadableResults(spark)

      And("the log is parsable")
      val (events, data) = assertLogProperties()
      
      testAction(input.schema, schema, events, data)
    }
  }
}
