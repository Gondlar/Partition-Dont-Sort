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

class NonSplitterSpec extends WavesSpec
    with DataFrameFixture with TempFolderFixture {

    "The NonSplitter" should {

        "create a single partition" in {
            When("we partition a data frame")
            new NonSplitter(df, tempDirectory.toString).partition()

            Then("there is exactly one partition")
            val partitions = tempDirectory.toFile().listFiles.filter(_.isDirectory())
            partitions.length should equal (1)
            val partition = partitions.head

            And("The partition is cleaned up")
            partition.getName() should not contain "="
            partition.listFiles.filter(_.getName.endsWith(".parquet")) should have size (1)

            And("We can read everything as a WavesTable")
            val newDf = spark.read.format("de.unikl.cs.dbis.waves").load(tempDirectory.toString)
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
