package de.unikl.cs.dbis.waves

import org.apache.spark.sql.util.CaseInsensitiveStringMap
import de.unikl.cs.dbis.waves.partitions.PartitionTreeHDFSInterface
import org.apache.spark.sql.AnalysisException
import org.apache.hadoop.fs.Path
import de.unikl.cs.dbis.waves.partitions.Bucket
import org.apache.spark.sql.types.StructType
import de.unikl.cs.dbis.waves.partitions.PartitionTree

import collection.JavaConverters._

class WavesTableSpec extends WavesSpec
  with DataFrameFixture with TempFolderFixture with PartitionTreeFixture
  with PartitionTreeMatchers {

  val emptyMap = CaseInsensitiveStringMap.empty()
  def schemaMap(schema: StructType) = {
    val json = new PartitionTree(schema).toJson
    val map = Map((WavesTable.PARTITION_TREE_OPTION, json)).asJava
    new CaseInsensitiveStringMap(map)
  }

  "A WavesTable" when {
    "we create it with a known schema" should {
      "have that schema" in {
          val table = WavesTable("test", spark, tempDirectory.toString(), emptyMap, schema)
          table.schema() should equal (schema)
      }
      "write that schema" in {
        val table = WavesTable("test", spark, tempDirectory.toString(), emptyMap, schema)
        table.writePartitionScheme()

        val written = PartitionTreeHDFSInterface(spark, tempDirectory.toString).read()
        written should not equal (None)
        written.get.globalSchema should equal (schema)
      }
    }
    "we create it without a schema" should {
      "load the schema from disk" in {
        PartitionTreeHDFSInterface(spark, tempDirectory.toString).write(bucketTree)
        val table = WavesTable("test", spark, tempDirectory.toString(), emptyMap)
        table.schema() should equal (schema)
      }
      "fail if there is no schema on disk" in {
        an [AnalysisException] shouldBe thrownBy (WavesTable("test", spark, tempDirectory.toString(), emptyMap))
      }
    }
    "we force the schema using an option" should {
      "prefer that schema over what is on disk" in {
        // No schema on disk, if it didn't prefer the schema option this would fail
        val table = WavesTable("test", spark, tempDirectory.toString(), schemaMap(schema))
        table.schema should equal (schema)
      }
      "prefer that schema over one passed as parameter" in {
        val table = WavesTable("test", spark, tempDirectory.toString(), schemaMap(schema), StructType(Seq.empty))
        table.schema() should equal (schema)
      }
    }
  }
  it can {
    "be truncated" in {
      Given("a table with a tree with multiple buckets")
      val table = WavesTable("test", spark, tempDirectory.toString(), emptyMap, schema)
      table.partitionTree = splitTree

      When("we truncate it")
      val dir = table.truncate

      Then("the tree is only a bucket")
      table.partitionTree should haveTheSameStructureAs(bucketTree)

      And("the returned folder is part of the new tree, not the old one")
      val newDir = table.partitionTree.root.asInstanceOf[Bucket[String]].folder(tempDirectory.toString())
      dir should equal (newDir)

      val oldDirs = splitTree.getBuckets().map(_.folder(tempDirectory.toString()))
      oldDirs shouldNot contain (dir)
    }
  }
}