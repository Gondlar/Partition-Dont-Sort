package de.unikl.cs.dbis.waves.partitions

import de.unikl.cs.dbis.waves.WavesSpec

import de.unikl.cs.dbis.waves.util.{PartitionFolder,PathKey}
import de.unikl.cs.dbis.waves.PartitionTreeFixture
import de.unikl.cs.dbis.waves.TempFolderFixture
import de.unikl.cs.dbis.waves.SparkFixture
import org.apache.hadoop.fs.Path

class PartitionTreeHDFSInterfaceSpec extends WavesSpec
    with PartitionTreeFixture with TempFolderFixture with SparkFixture {

    "A PartitionTreeHDFSInterface" should {
        "be constructable from spark" in {
          noException shouldBe thrownBy (PartitionTreeHDFSInterface(spark, tempDirectory.toString).read())
        }

        "be constructable from fs" in {
          val fs = new Path(tempDirectory.toString).getFileSystem(spark.sparkContext.hadoopConfiguration)
          noException shouldBe thrownBy (PartitionTreeHDFSInterface(fs, tempDirectory.toString).read())
        }

        "be identical after writing and reading" in {
          Given("A HDFSInterface and a partition tree")
          val interface = PartitionTreeHDFSInterface(spark, tempDirectory.toString)

          When("we write the tree and read it again")
          interface.write(spillTree)
          val read = interface.read()

          Then("the read tree should be identical to the one we wrote")
          read should equal (Some(spillTree))
        }

        "return None when reading from a non-existing file" in {
          val interface = PartitionTreeHDFSInterface(spark, "nonexisting")
          interface.read() shouldBe (None)
        }
    }
}