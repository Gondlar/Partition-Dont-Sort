package de.unikl.cs.dbis.waves

import org.scalatest.Inspectors._

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.hadoop.fs.Path

import de.unikl.cs.dbis.waves.partitions.PartitionTreeHDFSInterface
import de.unikl.cs.dbis.waves.partitions.{PartitionTree,Bucket,SplitByPresence,Present}
import de.unikl.cs.dbis.waves.partitions.visitors.operations._

import collection.JavaConverters._
import de.unikl.cs.dbis.waves.util.PathKey
import de.unikl.cs.dbis.waves.util.PartitionFolder
import de.unikl.cs.dbis.waves.partitions.Spill
import de.unikl.cs.dbis.waves.sort.NoSorter

class WavesTableSpec extends WavesSpec
  with DataFrameFixture with TempFolderFixture with PartitionTreeFixture
  with PartitionTreeMatchers {

  val emptyMap = CaseInsensitiveStringMap.empty()
  def schemaMap(schema: StructType) = {
    val map = Map((WavesTable.SCHEMA_OPTION, schema.json)).asJava
    new CaseInsensitiveStringMap(map)
  }

  "A WavesTable" when {
    "we create it with a known schema" should {
      "have that schema" in {
          val table = WavesTable("test", spark, tempDirectory, emptyMap, schema)
          table.schema() should equal (schema)
      }
      "write that schema" in {
        val table = WavesTable("test", spark, tempDirectory, emptyMap, schema)
        table.writePartitionScheme()

        val written = PartitionTreeHDFSInterface(spark, tempDirectory).read()
        written should not equal (None)
        written.get.globalSchema should equal (schema)
      }
    }
    "we create it without a schema" should {
      "load the schema from disk" in {
        PartitionTreeHDFSInterface(spark, tempDirectory).write(bucketTree)
        val table = WavesTable("test", spark, tempDirectory, emptyMap)
        table.schema() should equal (schema)
      }
      "fail if there is no schema on disk" in {
        an [AnalysisException] shouldBe thrownBy (WavesTable("test", spark, tempDirectory, emptyMap))
      }
    }
    "we force the schema using an option" should {
      "prefer that schema over what is on disk" in {
        // No schema on disk, if it didn't prefer the schema option this would fail
        val table = WavesTable("test", spark, tempDirectory, schemaMap(schema))
        table.schema should equal (schema)
      }
      "prefer that schema over one passed as parameter" in {
        val table = WavesTable("test", spark, tempDirectory, schemaMap(schema), StructType(Seq.empty))
        table.schema() should equal (schema)
      }
      "do so if everything is forced" in {
        import PartitionTree._
        val map = Map((WavesTable.SCHEMA_OPTION, schema.json), (WavesTable.SORT_OPTION, NoSorter.toJson), (WavesTable.PARTITION_OPTION, split.toJson)).asJava
        val options = new CaseInsensitiveStringMap(map)
        val table = WavesTable("test", spark, tempDirectory, options)
      }
    }
  }
  it can {
    "be truncated" in {
      Given("a table with a tree with multiple buckets")
      val table = WavesTable("test", spark, tempDirectory, emptyMap, schema)
      table.partitionTree = splitTree

      When("we truncate it")
      val dir = table.truncate

      Then("the tree is only a bucket")
      table.partitionTree should haveTheSameStructureAs(bucketTree)

      And("the returned folder is part of the new tree, not the old one")
      val newDir = table.partitionTree.root.asInstanceOf[Bucket[String]].folder(tempDirectory)
      dir should equal (newDir)

      val oldDirs = splitTree.buckets.map(_.folder(tempDirectory))
      oldDirs shouldNot contain (dir)
    }
  }
}

class WavesTableOperationsSpec extends WavesSpec
with RelationFixture with PartitionTreeFixture
with PartitionTreeMatchers {
  import WavesTable._

  def getTable = spark.read.waves(directory).getWavesTable.get

  "A WavesTable" can {
    "repartition its data" when {
      "its just a bucket" in {
        Given("a waves table and a desired shape")
        val table = getTable
        val shape = SplitByPresence("b.d", "foo", "bar")
        val tree = new PartitionTree(schema, NoSorter, shape)

        When("we repartition it")
        table.repartition(Seq.empty, shape.shape)

        Then("it has the desired shape")
        table.partitionTree should haveTheSameStructureAs (tree)
        PartitionTreeHDFSInterface(spark, table.basePath).read.get should equal (table.partitionTree)

        And("we can still read it as a WavesTable")
        spark.read.waves(table.basePath).collect() should contain theSameElementsAs (df.collect)
      }
      "it has been repartitioned" in {
        Given("a waves table that has been repartitioned and a desired shape")
        val table = getTable
        table.repartition(Seq.empty, SplitByPresence("a", (), ()))
        val shape = SplitByPresence("b.d", "foo", "bar")
        val tree = new PartitionTree(schema, NoSorter, shape)

        When("we repartition it")
        table.repartition(Seq.empty, shape.shape)

        Then("it has the desired shape")
        table.partitionTree should haveTheSameStructureAs (tree)
        PartitionTreeHDFSInterface(spark, table.basePath).read.get should equal (table.partitionTree)

        And("we can still read it as a WavesTable")
        spark.read.waves(table.basePath).collect() should contain theSameElementsAs (df.collect)
      }
      "we repartition only a subtree" in {
        Given("a waves table that has been repartitioned and a desired shape")
        val table = getTable
        table.repartition(Seq.empty, SplitByPresence("a", (), ()))
        val shape = SplitByPresence("b.d", "foo", "bar")
        val tree = new PartitionTree(schema, NoSorter, SplitByPresence("a", shape, Bucket("baz")))

        When("we repartition it")
        table.repartition(Seq(Present), shape.shape)

        Then("it has the desired shape")
        table.partitionTree should haveTheSameStructureAs (tree)
        PartitionTreeHDFSInterface(spark, table.basePath).read.get should equal (table.partitionTree)

        And("we can still read it as a WavesTable")
        spark.read.waves(table.basePath).collect() should contain theSameElementsAs (df.collect)
      }
    }
    "split its data" when {
      "it is just a bucket" in {
        Given("a waves table and a new split")
        val table = getTable
        val split = "b.d"
        val path = Seq.empty

        When("we split it")
        table.split(PathKey(split), path)

        Then("it has the expected shape")
        val shape = new PartitionTree(schema, NoSorter, SplitByPresence("b.d", "foo", "bar"))
        table.partitionTree should haveTheSameStructureAs (shape)
        PartitionTreeHDFSInterface(spark, table.basePath).read.get should equal (table.partitionTree)

        And("we can still read it as a WavesTable")
        spark.read.waves(table.basePath).collect() should contain theSameElementsAs (df.collect)
      }
      "we split a subtree" in {
        Given("a waves table that has been repartitioned and a new split")
        val table = getTable
        table.repartition(Seq.empty, SplitByPresence("a", (), ()))
        val split = "b.d"
        val path = Seq(Present)
        
        When("we split it")
        table.split(PathKey(split), path)
        
        Then("it has the expected shape")
        val shape = new PartitionTree(schema, NoSorter, SplitByPresence("a", SplitByPresence("b.d", "foo", "bar"), Bucket("baz")))
        table.partitionTree should haveTheSameStructureAs (shape)
        PartitionTreeHDFSInterface(spark, table.basePath).read.get should equal (table.partitionTree)

        And("we can still read it as a WavesTable")
        spark.read.waves(table.basePath).collect() should contain theSameElementsAs (df.collect)
      }
    }
    "unspill its data" when {
      "it has no spill nodes" in {
        Given("a waves table with just a bucket")
        val table = getTable
        val originalTree = new PartitionTree(table.schema, NoSorter, table.partitionTree.root)

        When("we unspill the data")
        table.unspill

        Then("nothing has changed")
        table.partitionTree should equal (originalTree)
        spark.read.waves(directory).collect() should contain theSameElementsAs (df.collect())
      }
      "its spill nodes are empty" in {
        Given("a waves table with an empty spill partition")
        val table = getTable
        implicit val fs = table.fs
        val spillFolder = PartitionFolder.makeFolder(table.basePath, false)
        val leafBucket = table.partitionTree.root
        spillFolder.mkdir
        val spillTree = Spill(leafBucket, Bucket(spillFolder.name))
        table.partitionTree = new PartitionTree(table.schema, NoSorter, spillTree)

        When("we unspill it")
        table.unspill

        Then("the spill bucket is no more")
        table.partitionTree should haveTheSameStructureAs (new PartitionTree(schema, NoSorter, leafBucket))
        spillFolder.isEmpty shouldBe (true)
        spark.read.waves(directory).collect() should contain theSameElementsAs (df.collect())
      }
      "it has an empty leaf and a spill bucket with content" in {
        Given("a waves table with an empty spill partition")
        val table = getTable
        implicit val fs = table.fs
        val leafFolder = PartitionFolder.makeFolder(table.basePath, false)
        leafFolder.mkdir
        val spillBucket = table.partitionTree.root.asInstanceOf[Bucket[String]]
        val spillTree = Spill(Bucket(leafFolder.name), spillBucket)
        table.partitionTree = new PartitionTree(table.schema, NoSorter, spillTree)

        When("we unspill it")
        table.unspill

        Then("the data was moved to the bucket")
        table.partitionTree should haveTheSameStructureAs (new PartitionTree(schema, NoSorter, spillBucket))
        table.partitionTree.root.asInstanceOf[Bucket[String]].folder(table.basePath).isEmpty shouldBe (false)
        spark.read.waves(directory).collect() should contain theSameElementsAs (df.collect())
      }
      "it needs to combine the leaf and the spill bucket" in {
        Given("a waves table with a spill bucket and a leaf bucket")
        val table = getTable
        table.split(PathKey("b.d"))
        val split = table.partitionTree.root.asInstanceOf[SplitByPresence[String]]
        val leaf = split.absentKey
        val spill = split.presentKey.asInstanceOf[Bucket[String]]
        table.partitionTree = new PartitionTree(schema, NoSorter, Spill(leaf, spill))

        When("we unspill it")
        table.unspill

        Then("the data was moved to the bucket")
        table.partitionTree should haveTheSameStructureAs(new PartitionTree(schema, NoSorter, leaf))
        spark.read.waves(directory).collect() should contain theSameElementsAs (df.collect())
      }
    }
  }
}