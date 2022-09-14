package de.unikl.cs.dbis.waves.split

import org.scalatest.Inspectors._
import de.unikl.cs.dbis.waves.WavesSpec
import de.unikl.cs.dbis.waves.DataFrameFixture

import java.nio.file.Files
import java.nio.file.Path
import org.apache.commons.io.FileUtils

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Column}

class EvenSplitterSpec extends WavesSpec
    with DataFrameFixture {

    "The EvenSplitter" should {

        "split a dataframe into partitions" in {
            When("we partition a data frame")
            // noException shouldBe thrownBy (new EvenSplitter(df, 2, baseDirectory.toString).partition())
            new EvenSplitter(df, 2, baseDirectory.toString).partition()

            Then("there are more than two partitions")
            val partitions = baseDirectory.toFile().listFiles.filter(_.isDirectory())
            partitions.length shouldBe >= (2)

            And("The partitions are cleaned up")
            forAll (partitions) { dir =>
              dir.getName() should not contain "="
              dir.listFiles.filter(_.getName.endsWith(".parquet")) should have size (1)
            }

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
