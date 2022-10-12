package de.unikl.cs.dbis.waves.split

import org.scalatest.Inspectors._
import de.unikl.cs.dbis.waves.WavesSpec
import de.unikl.cs.dbis.waves.RelationFixture
import de.unikl.cs.dbis.waves.TempFolderFixture
import de.unikl.cs.dbis.waves.PartitionTreeFixture
import de.unikl.cs.dbis.waves.PartitionTreeMatchers

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Column}
import org.apache.hadoop.fs.Path

import de.unikl.cs.dbis.waves.partitions.{PartitionTree,SplitByPresence,Bucket,Absent}
import de.unikl.cs.dbis.waves.partitions.PartitionTreeHDFSInterface
import de.unikl.cs.dbis.waves.partitions.visitors.operations._
import de.unikl.cs.dbis.waves.sort.NoSorter

import de.unikl.cs.dbis.waves.WavesTable._

class PredefinedSplitterSpec extends WavesSpec
  with RelationFixture with PartitionTreeFixture with TempFolderFixture
  with PartitionTreeMatchers {

  "The PredefinedSplitter" can {
    "split a dataframe into predefined partitions" in {
      Given("a DataFrame and a PartitionTree")
      val splitter = new PredefinedSplitter(split)
      splitter.prepare(df, tempDirectory.toString())
    
      When("we partition the data frame")
      splitter.partition()

      Then("the written partition tree looks as defined")
      val fs = new Path(tempDirectory.toString()).getFileSystem(spark.sparkContext.hadoopConfiguration)
      val result = PartitionTreeHDFSInterface(fs, tempDirectory.toString()).read()
      result should not equal (None)
      result.get should haveTheSameStructureAs(splitTree)

      And("We can read everything as a WavesTable")
      val newDf = spark.read.waves(tempDirectory.toString)
      newDf.collect() should contain theSameElementsAs (df.collect())

      And("we recieve the correct data when selecting one attribute")
      compareFilteredDataframe(newDf, df, col("a").isNull)
      compareFilteredDataframe(newDf, df, col("b").isNotNull)
      compareFilteredDataframe(newDf, df, col("b.d").isNull)
      compareFilteredDataframe(newDf, df, col("b.d").isNotNull)
    }
    "extend an existing partition tree" in {
      Given("a Waves Table and a new split")
      val data = spark.read.waves(directory)
      val interface = PartitionTreeHDFSInterface(spark, directory)
      interface.read().get should haveTheSameStructureAs(bucketTree)

      val splitter = new PredefinedSplitter(split)
      splitter.prepare(data, directory)
      
      When("we partition it")
      splitter.partition()

      Then("the written partition tree looks as defined")
      val tree = interface.read()
      tree should not equal (None)
      tree.get should haveTheSameStructureAs (splitTree)

      When("we perform a further split")
      val newShape = SplitByPresence("a", "test1", "test2")
      val splitter2 = new PredefinedSplitter(newShape, Seq(Absent))
      splitter2.prepare(data.filter(col(split.key.toSpark).isNull), directory)
      splitter2.partition()

      Then("the written partition tree looks as defined")
      val tree2 = interface.read()
      tree2 should not equal (None)
      splitTree.replace(Seq(Absent), newShape)
      tree2.get should haveTheSameStructureAs (splitTree)

      And("We can read everything as a WavesTable")
      val newDf = spark.read.waves(directory)
      newDf.collect() should contain theSameElementsAs (df.collect())

      And("we recieve the correct data when selecting one attribute")
      compareFilteredDataframe(newDf, df, col("a").isNull)
      compareFilteredDataframe(newDf, df, col("b").isNotNull)
      compareFilteredDataframe(newDf, df, col("b.d").isNull)
      compareFilteredDataframe(newDf, df, col("b.d").isNotNull)
    }
    "merge an existing tree's partitions" in {
      Given("a tree on disk")
      val splitter = new PredefinedSplitter(split)
      splitter.prepare(df, tempDirectory.toString()).partition()

      When("we merge that tree")
      val merger = new PredefinedSplitter(bucket)
      merger.prepare(df,tempDirectory.toString()).partition()

      Then("the written partition tree looks as defined")
      val tree = PartitionTreeHDFSInterface(spark, tempDirectory.toString()).read()
      tree should not equal (None)
      tree.get should haveTheSameStructureAs (bucketTree)

      And("We can read everything as a WavesTable")
      val newDf = spark.read.waves(tempDirectory.toString())
      newDf.collect() should contain theSameElementsAs (df.collect())

      And("we recieve the correct data when selecting one attribute")
      compareFilteredDataframe(newDf, df, col("a").isNull)
      compareFilteredDataframe(newDf, df, col("b").isNotNull)
      compareFilteredDataframe(newDf, df, col("b.d").isNull)
      compareFilteredDataframe(newDf, df, col("b.d").isNotNull)
    }
    "split when there are Spill nodes in the new subtree" in {
      Given("a DataFrame and a PartitionTree")
      val splitter = new PredefinedSplitter(spill)
      splitter.prepare(df, tempDirectory.toString())
    
      When("we partition the data frame")
      splitter.partition()

      Then("the written partition tree looks as defined")
      val fs = new Path(tempDirectory.toString()).getFileSystem(spark.sparkContext.hadoopConfiguration)
      val result = PartitionTreeHDFSInterface(fs, tempDirectory.toString()).read()
      result should not equal (None)
      result.get should haveTheSameStructureAs(spillTree)

      And("We can read everything as a WavesTable")
      val newDf = spark.read.waves(tempDirectory.toString)
      newDf.collect() should contain theSameElementsAs (df.collect())

      And("we recieve the correct data when selecting one attribute")
      compareFilteredDataframe(newDf, df, col("a").isNull)
      compareFilteredDataframe(newDf, df, col("b").isNotNull)
      compareFilteredDataframe(newDf, df, col("b.d").isNull)
      compareFilteredDataframe(newDf, df, col("b.d").isNotNull)
    }
    "handle nested splits" in {
      Given("a DataFrame and a PartitionTree")
      val shape = SplitByPresence("a", Bucket("foo"), SplitByPresence("b.d", "bar", "baz"))
      val splitter = new PredefinedSplitter(shape)
      splitter.prepare(df, tempDirectory.toString())
      
      When("we partition it")
      splitter.partition()

      Then("the written partition tree looks as defined")
      val expextedTree = new PartitionTree(schema, NoSorter, shape)
      val result = PartitionTreeHDFSInterface(spark, tempDirectory.toString()).read()
      result should not equal (None)
      result.get should haveTheSameStructureAs (expextedTree)

      And("We can read everything as a WavesTable")
      val newDf = spark.read.waves(tempDirectory.toString)
      newDf.collect() should contain theSameElementsAs (df.collect())

      And("we recieve the correct data when selecting one attribute")
      compareFilteredDataframe(newDf, df, col("a").isNull)
      compareFilteredDataframe(newDf, df, col("b").isNotNull)
      compareFilteredDataframe(newDf, df, col("b.d").isNull)
      compareFilteredDataframe(newDf, df, col("b.d").isNotNull)
    }
  }
  it should {
    "refuse to prepare an empty directory given a subpath" in {
      val splitter = new PredefinedSplitter(spill, Seq(Absent))
      an [IllegalArgumentException] shouldBe thrownBy (splitter.prepare(df, tempDirectory.toString))
    }
  }

  def compareFilteredDataframe(lhs: DataFrame, rhs: DataFrame, col: Column)
    = lhs.filter(col).collect() should contain theSameElementsAs (rhs.filter(col).collect())
}
