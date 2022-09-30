package de.unikl.cs.dbis.waves

import org.apache.spark.sql.SaveMode
import de.unikl.cs.dbis.waves.partitions.PartitionTreeHDFSInterface

import WavesTable._

class WavesDataFrameSpec extends WavesSpec with RelationFixture with PartitionTreeFixture
with PartitionTreeMatchers {

  "A DataFrame" when {
    "it reads directly from a WavesTable" should {
      "retrieve that table" in {
          val df = spark.read.waves(directory)
          df.isWavesTable should be (true)
          df.getWavesTable shouldBe defined
          df.getWavesTable.get.name should equal (s"waves $directory")
      }
    }
    "it indirectly reads from a WavesTable" should {
      "not retrieve a table" in {
        val df = spark.read.waves(directory).select("a")
        df.isWavesTable should be (false)
        df.getWavesTable shouldBe empty
      }
    }
    "it doesn't read from a  WavesTable" should {
      "not retrieve a table" in {
        df.isWavesTable should be (false)
        df.getWavesTable shouldBe empty
      }
    }
    "writing to disk" should {
      "have the correct schema" in {
        When("we write da dataframe to disk")
        df.schema should equal (schema)
        df.write.mode(SaveMode.Overwrite).waves(directory, df.schema)

        Then("the schema on disk is the dataframe's schema")
        val result = PartitionTreeHDFSInterface(spark, directory).read()
        result should not equal (None)
        result.get should haveTheSameStructureAs (bucketTree)
      }
    }
    "reading from disk" should {
      "read that data" in {
        spark.read.waves(directory).collect() should contain theSameElementsAs (df.collect())
      }
    }
  }
}