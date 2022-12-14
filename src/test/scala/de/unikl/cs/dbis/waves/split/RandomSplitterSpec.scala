package de.unikl.cs.dbis.waves.split

import org.scalatest.Inspectors._
import de.unikl.cs.dbis.waves.WavesSpec
import de.unikl.cs.dbis.waves.DataFrameFixture
import de.unikl.cs.dbis.waves.TempFolderFixture

import java.nio.file.Files
import org.apache.hadoop.fs.Path
import org.apache.commons.io.FileUtils

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Column}

import de.unikl.cs.dbis.waves.WavesTable._
import de.unikl.cs.dbis.waves.partitions.PartitionTreeHDFSInterface
import de.unikl.cs.dbis.waves.util.PartitionFolder

class RandomSplitterSpec extends WavesSpec
  with DataFrameFixture with TempFolderFixture {

  "The RandomSplitter" should {
    "create disjoint partitions" in {
      When("we partition a data frame")
      val partitionCount = 2
      new RandomSplitter(partitionCount).prepare(df, tempDirectory.toString).partition()

      Then("there is the correct number of partitions")
      implicit val fs = PartitionTreeHDFSInterface(spark, tempDirectory.toString).fs
      val partitions = PartitionFolder.allInDirectory(new Path(tempDirectory.toString)).toSeq
      partitions.length shouldBe <= (partitionCount)
      if (partitions.length < partitionCount) {
        alert(s"Got less than $partitionCount partitions. This can be random chance, but is a bug if it persists")
      }

      And("The partitions are cleaned up")
      forAll (partitions) { partition =>
        partition.name should not contain "="
        partition.parquetFiles.toSeq should have size (1)
      }

      And("We can read everything as a WavesTable")
      val newDf = spark.read.waves(tempDirectory.toString)
      newDf.collect() should contain theSameElementsAs (df.collect())

      And("we can recieve the correct data when selecting one attribute")
      compareFilteredDataframe(newDf, df, col("a").isNull)
      compareFilteredDataframe(newDf, df, col("b").isNotNull)
      compareFilteredDataframe(newDf, df, col("b.d").isNull)
    }
  }

  def compareFilteredDataframe(lhs: DataFrame, rhs: DataFrame, col: Column)
    = lhs.filter(col).collect() should contain theSameElementsAs (rhs.filter(col).collect())
}
