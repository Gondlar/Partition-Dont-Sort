package de.unikl.cs.dbis.waves.partitions

import de.unikl.cs.dbis.waves.WavesSpec

import de.unikl.cs.dbis.waves.util.{PartitionFolder,PathKey}
import de.unikl.cs.dbis.waves.PartitionTrees
import de.unikl.cs.dbis.waves.Relation
import org.apache.hadoop.fs.Path

class PartitionTreeHDFSInterfaceSpec extends WavesSpec
    with PartitionTrees with Relation {

    "A PartitionTreeHDFSInterface" should {
        "be constructable from spark" in {
          noException shouldBe thrownBy (PartitionTreeHDFSInterface(spark, directory).read())
        }

        "be constructable from fs" in {
          val fs = new Path(directory).getFileSystem(spark.sparkContext.hadoopConfiguration)
          noException shouldBe thrownBy (PartitionTreeHDFSInterface(fs, directory).read())
        }

        "be identical after writing and reading" in {
          Given("A HDFSInterface and a partition tree")
          val interface = PartitionTreeHDFSInterface(spark, directory)

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