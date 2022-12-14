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
import de.unikl.cs.dbis.waves.util.PartitionFolder
import de.unikl.cs.dbis.waves.partitions.PartitionTreeHDFSInterface

class EvenSplitterSpec extends WavesSpec
  with DataFrameFixture with TempFolderFixture
  with SplitterBehavior {

  "The EvenSplitter" should {
    behave like deterministicSplitter(new EvenSplitter(2))
    "create disjoint partitions" in {
      When("we partition a data frame")
      val partitionCount = 4
      new EvenSplitter(partitionCount).prepare(df, tempDirectory.toString).partition()

      Then("there is the correct number of partitions")
      implicit val fs = PartitionTreeHDFSInterface.apply(spark, tempDirectory.toString).fs
      val partitions = PartitionFolder.allInDirectory(new Path(tempDirectory.toString)).toSeq
      partitions.length should equal (partitionCount)

      And("The partitions are cleaned up")      
      forAll (partitions) { partition =>
        partition.name should not contain "="
        val files = partition.parquetFiles.toSeq
        files should have size (1)
        spark.read.parquet(files.head.toString).count() should equal (2)
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
