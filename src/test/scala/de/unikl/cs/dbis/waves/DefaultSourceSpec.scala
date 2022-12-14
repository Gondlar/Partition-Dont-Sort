package de.unikl.cs.dbis.waves

import org.apache.spark.sql.util.CaseInsensitiveStringMap
import de.unikl.cs.dbis.waves.partitions.PartitionTreeHDFSInterface
import org.apache.spark.sql.AnalysisException

import collection.JavaConverters._

class DefaultSourceSpec extends WavesSpec
with DataFrameFixture with TempFolderFixture with PartitionTreeFixture
with PartitionTreeMatchers {

  def generatePathOption(path: String)
    = new CaseInsensitiveStringMap(Map(("path", path.toString())).asJava)

  "The DefaultSource" can {
    "infer a schema from disk" in {
      PartitionTreeHDFSInterface(spark, tempDirectory).write(bucketTree)
      DefaultSource().inferSchema(generatePathOption(tempDirectory)) should equal (schema)
    }
    "construct a new table" in {
      val result = DefaultSource().getTable(schema, Array.empty, generatePathOption(tempDirectory))
      result shouldBe a [WavesTable]
      val table = result.asInstanceOf[WavesTable]
      table.schema() should equal (schema)
      table.name should startWith ("waves ")
      table.basePath should equal (tempDirectory)
      table.partitionTree should haveTheSameStructureAs (bucketTree)
    }
    "load a table from disk" in {
      Given("a partition tree on disk")
      PartitionTreeHDFSInterface(spark, tempDirectory).write(splitTree)
      val pathOption = generatePathOption(tempDirectory)

      When("we load the table")
      val source = DefaultSource()
      val schema = source.inferSchema(pathOption)
      val result = source.getTable(schema, Array.empty, pathOption)
      result shouldBe a [WavesTable]
      val table = result.asInstanceOf[WavesTable]

      Then("the table has the tree from disk")
      table.schema() should equal (schema)
      table.name should startWith ("waves ")
      table.basePath should equal (tempDirectory)
      table.partitionTree should haveTheSameStructureAs (splitTree)
    }
  }
  it should {
    "throw an error when inferring a schema for a non-existing table" in {
      an [AnalysisException] shouldBe thrownBy (DefaultSource().inferSchema(generatePathOption(tempDirectory)))
    }
  }
}