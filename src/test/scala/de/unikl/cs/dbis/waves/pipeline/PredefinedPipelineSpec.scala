package de.unikl.cs.dbis.waves.pipeline

import org.scalatest.Inspectors._
import de.unikl.cs.dbis.waves.WavesSpec
import de.unikl.cs.dbis.waves.RelationFixture
import de.unikl.cs.dbis.waves.TempFolderFixture
import de.unikl.cs.dbis.waves.PartitionTreeFixture
import de.unikl.cs.dbis.waves.PartitionTreeMatchers
import de.unikl.cs.dbis.waves.split.SplitterBehavior

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Column}

import de.unikl.cs.dbis.waves.partitions.{PartitionTree,SplitByPresence,Bucket,Present,Absent}
import de.unikl.cs.dbis.waves.partitions.TreeNode.AnyNode
import de.unikl.cs.dbis.waves.partitions.PartitionTreeHDFSInterface
import de.unikl.cs.dbis.waves.partitions.visitors.operations._
import de.unikl.cs.dbis.waves.sort.NoSorter

import de.unikl.cs.dbis.waves.WavesTable._
import de.unikl.cs.dbis.waves.pipeline.split.Predefined
import de.unikl.cs.dbis.waves.pipeline.sink.DataframeSink
import de.unikl.cs.dbis.waves.pipeline.util.BucketsFromShape

class PredefinedPipelineSpec extends WavesSpec
  with RelationFixture with PartitionTreeFixture with TempFolderFixture
  with SplitterBehavior with PartitionTreeMatchers {

  def makePipeline(shape: AnyNode[String])
    = new Pipeline(Seq(Predefined(shape.shape), BucketsFromShape), DataframeSink)

  "A Pipeline with a predefined split" can {
    behave like unpreparedSplitter(makePipeline(Bucket("foo")))
    "split a dataframe into predefined buckets" in {
      Given("a DataFrame and a PartitionTree")
      val splitter = makePipeline(split)
      splitter.prepare(df, tempDirectory)
    
      When("we partition the data frame")
      splitter.partition()

      Then("the written partition tree looks as defined")
      val fs = getFS(spark)
      val result = PartitionTreeHDFSInterface(fs, tempDirectory).read()
      result should not equal (None)
      result.get should haveTheSameStructureAs(splitTree)

      And("We can read everything as a WavesTable")
      val newDf = spark.read.waves(tempDirectory)
      newDf.collect() should contain theSameElementsAs (df.collect())

      And("we recieve the correct data when selecting one attribute")
      compareFilteredDataframe(newDf, df, col("a").isNull)
      compareFilteredDataframe(newDf, df, col("b").isNotNull)
      compareFilteredDataframe(newDf, df, col("b.d").isNull)
      compareFilteredDataframe(newDf, df, col("b.d").isNotNull)
    }
    "split when there are Spill nodes in the new subtree" in {
      Given("a DataFrame and a PartitionTree")
      val splitter = makePipeline(spill)
      splitter.prepare(df, tempDirectory)
    
      When("we partition the data frame")
      splitter.partition()

      Then("the written partition tree looks as defined")
      val fs = getFS(spark)
      val result = PartitionTreeHDFSInterface(fs, tempDirectory).read()
      result should not equal (None)
      result.get should haveTheSameStructureAs(spillTree)

      And("We can read everything as a WavesTable")
      val newDf = spark.read.waves(tempDirectory)
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
      val splitter = makePipeline(shape)
      splitter.prepare(df, tempDirectory)
      
      When("we partition it")
      splitter.partition()

      Then("the written partition tree looks as defined")
      val expextedTree = new PartitionTree(schema, NoSorter, shape)
      val result = PartitionTreeHDFSInterface(spark, tempDirectory).read()
      result should not equal (None)
      result.get should haveTheSameStructureAs (expextedTree)

      And("We can read everything as a WavesTable")
      val newDf = spark.read.waves(tempDirectory)
      newDf.collect() should contain theSameElementsAs (df.collect())

      And("we recieve the correct data when selecting one attribute")
      compareFilteredDataframe(newDf, df, col("a").isNull)
      compareFilteredDataframe(newDf, df, col("b").isNotNull)
      compareFilteredDataframe(newDf, df, col("b.d").isNull)
      compareFilteredDataframe(newDf, df, col("b.d").isNotNull)
    }
  }

  def compareFilteredDataframe(lhs: DataFrame, rhs: DataFrame, col: Column)
    = lhs.filter(col).collect() should contain theSameElementsAs (rhs.filter(col).collect())
}
