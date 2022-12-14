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

class NonSplitterSpec extends WavesSpec
    with DataFrameFixture with TempFolderFixture {

    "The NonSplitter" should {

        "create a single partition" in {
            When("we partition a data frame")
            new NonSplitter().prepare(df, tempDirectory).partition()

            Then("there is exactly one partition")
            implicit val fs = getFS(spark)
            val partitions = PartitionFolder.allInDirectory(tempDirectory).toSeq
            partitions.length should equal (1)
            val partition = partitions.head

            And("The partition is cleaned up")
            partition.name should not contain "="
            partition.parquetFiles.toSeq should have size (1)

            And("We can read everything as a WavesTable")
            val newDf = spark.read.waves(tempDirectory)
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
