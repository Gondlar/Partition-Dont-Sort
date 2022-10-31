package de.unikl.cs.dbis.waves.split

import org.scalatest.Inspectors._
import de.unikl.cs.dbis.waves.WavesSpec
import de.unikl.cs.dbis.waves.DataFrameFixture
import de.unikl.cs.dbis.waves.TempFolderFixture

import java.nio.file.Files
import java.nio.file.Path
import org.apache.commons.io.FileUtils

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Column}

import de.unikl.cs.dbis.waves.WavesTable._

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
      val partitions = tempDirectory.toFile().listFiles.filter(_.isDirectory())
      partitions.length should equal (partitionCount)

      And("The partitions are cleaned up")
      forAll (partitions) { partition =>
        partition.getName() should not contain "="
        val files = partition.listFiles.filter(_.getName.endsWith(".parquet"))
        files should have size (1)
        spark.read.parquet(files.head.getPath()).count() should equal (2)
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
