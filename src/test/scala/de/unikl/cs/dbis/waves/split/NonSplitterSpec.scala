package de.unikl.cs.dbis.waves.split

import org.scalatest.Inspectors._
import de.unikl.cs.dbis.waves.WavesSpec
import de.unikl.cs.dbis.waves.DataFrameFixture

import java.nio.file.Files
import java.nio.file.Path
import org.apache.commons.io.FileUtils

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Column}

class NonSplitterSpec extends WavesSpec
    with DataFrameFixture {

    "The NonSplitter" should {

        "create a single partition" in {
            When("we partition a data frame")
            new NonSplitter(df, baseDirectory.toString).partition()

            Then("there is exactly one partition")
            val partitions = baseDirectory.toFile().listFiles.filter(_.isDirectory())
            partitions.length should equal (1)
            val partition = partitions.head

            And("The partition is cleaned up")
            partition.getName() should not contain "="
            partition.listFiles.filter(_.getName.endsWith(".parquet")) should have size (1)

            And("We can read everything as a WavesTable")
            val newDf = spark.read.format("de.unikl.cs.dbis.waves").load(baseDirectory.toString)
            newDf.collect() should contain theSameElementsAs (df.collect())

            And("we can recieve the correct data when selecting one attribute")
            compareFilteredDataframe(newDf, df, col("a").isNull)
            compareFilteredDataframe(newDf, df, col("b").isNotNull)
            compareFilteredDataframe(newDf, df, col("b.d").isNull)
        }
    }

    def compareFilteredDataframe(lhs: DataFrame, rhs: DataFrame, col: Column)
      = lhs.filter(col).collect() should contain theSameElementsAs (rhs.filter(col).collect())

    var baseDirectory : Path = null
    override protected def beforeEach() = {
      super.beforeEach()
      baseDirectory = Files.createTempDirectory("evenPartitonTest")
      FileUtils.deleteQuietly(baseDirectory.toFile()) // We need just a name, not the dir itself
    }
    override protected def afterEach(): Unit = {
      super.afterEach()
      FileUtils.deleteQuietly(baseDirectory.toFile())
    }
}