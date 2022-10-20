package de.unikl.cs.dbis.waves

import org.apache.spark.sql.SaveMode
import de.unikl.cs.dbis.waves.partitions.PartitionTreeHDFSInterface

import WavesTable._
import de.unikl.cs.dbis.waves.split.Splitter
import org.apache.spark.sql.DataFrame
import de.unikl.cs.dbis.waves.sort.{Sorter,NoSorter}

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
        df.write.mode(SaveMode.Overwrite)
                .sorter(NoSorter)
                .partition(bucket)
                .waves(directory, df.schema)

        Then("the schema on disk is the dataframe's schema")
        val result = PartitionTreeHDFSInterface(spark, directory).read()
        result should not equal (None)
        result.get should haveTheSameStructureAs (bucketTree)
      }
      "use slow write correctly" in {
        val splitter = new MockSplitter
        df.saveAsWaves(splitter, "test")
        splitter.preparedData should equal (df)
        splitter.preparedPath should equal ("test")
        splitter.partitionCalled should be (true)
      }
      "use partition correctly" in {
        val splitter = new MockSplitter
        val table = spark.read.waves(directory).getWavesTable.get
        table.repartition(splitter)
        splitter.preparedData.collect should contain theSameElementsAs (df.collect())
        splitter.preparedPath should equal (table.basePath)
        splitter.partitionCalled should be (true)
      }
    }
    "reading from disk" should {
      "read that data" in {
        spark.read.waves(directory).collect() should contain theSameElementsAs (df.collect())
      }
    }
  }

  class MockSplitter() extends Splitter[Unit] {
    var preparedData: DataFrame = null
    var preparedPath: String = null
    var partitionCalled = false

    override def prepare(df: DataFrame, path: String): Splitter[Unit] = {
      preparedData = df
      preparedPath = path
      this
    }
    override def partition(): Unit = {
      preparedData shouldNot be (null)
      partitionCalled = true
    }

    // do not need
    override def isPrepared: Boolean = ???
    override def getPath: String = ???
    override def doFinalize(enabled: Boolean): Splitter[Unit] = ???
    override def finalizeEnabled: Boolean = ???
    override def sortWith(sorter: Sorter): Splitter[Unit] = ???
    override def modifySchema(enabled: Boolean): Splitter[Unit] = ???
    override protected def load(context: Unit): DataFrame = ???
  }
}