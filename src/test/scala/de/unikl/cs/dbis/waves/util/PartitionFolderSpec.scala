package de.unikl.cs.dbis.waves.util

import de.unikl.cs.dbis.waves.WavesSpec
import de.unikl.cs.dbis.waves.TempFolderFixture
import de.unikl.cs.dbis.waves.SparkFixture

import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem

class PartitonFolderSpec extends WavesSpec
with TempFolderFixture with SparkFixture {

    "A PartitionFolder" when {
        "temporary" should {
            "be in the temp directory" in {
                val folder = new PartitionFolder("test", "foo", true)
                folder.filename should equal ("test/tmp/foo")
                folder.file.toString should equal ("test/tmp/foo")
            }
            "equal itself" in {
                val folder = new PartitionFolder("test", "foo", true)
                val folder2 = new PartitionFolder("test", "foo", true)
                (folder == folder) should be (true)
                (folder == folder2) should be (true)
                folder.hashCode() should equal (folder2.hashCode)
            }
            "not equal its non-temporary version" in {
                val folder = new PartitionFolder("test", "foo", true)
                (folder == new PartitionFolder("test", "foo", false)) should be (false)
            }
            "not equal other folders" in {
                val folder = new PartitionFolder("test", "foo", true)
                (folder == new PartitionFolder("test", "bar", true)) should be (false)
                (folder == new PartitionFolder("test2", "foo", true)) should be (false)
            }
        }
        "non-temporary" should {
            "not be in the temp directory" in {
                val folder = new PartitionFolder("test", "foo", false)
                folder.filename should equal ("test/foo")
                folder.file.toString should equal ("test/foo")
            }
            "equal itself" in {
                val folder = new PartitionFolder("test", "foo", false)
                val folder2 = new PartitionFolder("test", "foo", false)
                (folder == folder) should be (true)
                (folder == folder2) should be (true)
                folder.hashCode() should equal (folder2.hashCode)
            }
            "not equal its temporary version" in {
                val folder = new PartitionFolder("test", "foo", false)
                (folder == new PartitionFolder("test", "foo", true)) should be (false)
            }
            "not equal other folders" in {
                val folder = new PartitionFolder("test", "foo", false)
                (folder == new PartitionFolder("test", "bar", false)) should be (false)
                (folder == new PartitionFolder("test2", "foo", false)) should be (false)
            }
            "perform filesystem operations" in {
                implicit val fs = getFS(spark)
                
                When("we create a folder object")
                val folder = new PartitionFolder(tempDirectory, "test", false)
                
                Then("it does not exist on file")
                folder.exists should be (false)
                
                And("we can create it")
                folder.mkdir
                val createdLoc = folder.file
                fs.exists(createdLoc) should be (true)
                folder.exists should be (true)

                And("we can move it")
                folder.mv(newName = folder.name+"123")
                fs.exists(createdLoc) should be (false)
                val movedLoc = folder.file
                fs.exists(movedLoc) should be (true)
                folder.exists should be (true)

                And("we can rename it")
                folder.rename(folder.name+"456")
                fs.exists(movedLoc) should be (false)
                val renamedLoc = folder.file
                fs.exists(renamedLoc) should be (true)
                folder.exists should be (true)

                And("we can move it to temp")
                folder.moveBetweenTemp
                fs.exists(renamedLoc) should be (false)
                val tempLoc = folder.file
                fs.exists(tempLoc) should be (true)
                folder.exists should be (true)
                folder.filename should include ("/tmp/")

                And("we can delete it again")
                folder.delete
                fs.exists(tempLoc) should be (false)
                folder.exists should be (false)
            }
            "copy contents from another directory" in {
              implicit val fs = getFS(spark)
              val file = "/foo.txt"
              val dir = "/subdir"

              Given("A directory with contents")
              val folderWithContent = new PartitionFolder(tempDirectory, "full", false)
              folderWithContent.mkdir
              val writer = fs.create(new Path(folderWithContent.filename + file))
              writer.writeInt(5)
              writer.writeInt(42)
              writer.writeInt(1337)
              writer.close()
              fs.mkdirs(new Path(folderWithContent.filename+dir))

              And("an empty dir")
              val folderWithoutContent = new PartitionFolder(tempDirectory, "empty", false)
              folderWithoutContent.mkdir

              Then("that directory should have the content's size")
              folderWithContent.diskSize should equal (12)

              And("the empty one has size 0")
              folderWithoutContent.diskSize should equal (0)

              When("we copy the contents over")
              folderWithoutContent.copyContentsFrom(folderWithContent)

              Then("both folders have the same size")
              folderWithoutContent.diskSize should equal (folderWithContent.diskSize)
              folderWithoutContent.diskSize should equal (12)

              And("the file can be read from the formerly empty dir")
              val reader = fs.open(new Path(folderWithoutContent.filename + file))
              reader.readInt() should equal(5)
              reader.readInt() should equal(42)
              reader.readInt() should equal(1337)
              reader.close()
              fs.exists(new Path(folderWithoutContent.filename+dir)) shouldBe (false)
            }
            "be empty" in {
                implicit val fs = getFS(spark)

                Given("an empty folder")
                val folder = new PartitionFolder(tempDirectory, "foo", false)
                folder.mkdir

                Then("it should be empty")
                folder.isEmpty shouldBe (true)

                When("we put a non-parquet file inside it")
                placeFile(folder, "foo.txt")

                Then("it is still empty")
                folder.isEmpty shouldBe (true)

                When("we put a parquet file in it")
                placeFile(folder, "bar.parquet")

                Then("it no longer empty")
                folder.isEmpty shouldBe (false)
            }
            "list all of its parquet files" in {
              Given("an empty folder")
              val folder = PartitionFolder.makeFolder(tempDirectory, false)
              implicit val fs = folder.filesystem(spark)
              folder.mkdir

              When("we list all parquet files")
              val files = folder.parquetFiles.toSeq

              Then("there should be no files")
              files shouldBe empty

              When("we place files in the folder")
              placeFile(folder, "foo.txt")
              placeFile(folder, "bar.parquet")
              placeFile(folder, "foobar.parquet")
              placeFile(folder, "parquet.txt")

              Then("we get a list of all parquet files")
              folder.parquetFiles.toSeq should contain theSameElementsAs (Seq(
                new Path(s"file:${folder.filename}/bar.parquet"),
                new Path(s"file:${folder.filename}/foobar.parquet")
              ))
            }
        }
        "being created" should {
            "be in the specified state" in {
                PartitionFolder.makeFolder("test", true) should have (
                    'baseDir ("test"),
                    'isTemporary (true)
                )
                PartitionFolder.makeFolder("test2", false) should have (
                    'baseDir ("test2"),
                    'isTemporary (false)
                )
                new PartitionFolder("foo", "bar", false) should have (
                    'baseDir ("foo"),
                    'name ("bar"),
                    'isTemporary (false)
                )
            }
            "find no directories in an empty folder" in {
              implicit val fs = getFS(spark)
              fs.mkdirs(new Path(tempDirectory))
              PartitionFolder.allInDirectory(tempDirectory).toSeq shouldBe empty
            }
            "find all directories in a folder" in {
              Given("a folder with subfolders")
              implicit val fs = getFS(spark)
              fs.mkdirs(new Path(tempDirectory))
              placeFolder(tempDirectory, "foo")
              placeFolder(tempDirectory, "bar")
              placeFolder(tempDirectory, PartitionFolder.TEMP_DIR)

              When("we list all folders")
              val folders = PartitionFolder.allInDirectory(tempDirectory).toSeq

              Then("we find all folders except the temp folder")
              folders should contain theSameElementsAs Seq(
                new PartitionFolder(tempDirectory, "foo", false),
                new PartitionFolder(tempDirectory, "bar", false)
              )
            }
        }
    }

    def placeFile(folder: PartitionFolder, name: String)(implicit fs: FileSystem) = {
      val writer = fs.create(new Path(s"${folder.filename}/$name"))
      writer.writeInt(5)
      writer.close
    }

    def placeFolder(parent: String, name: String)(implicit fs: FileSystem)
      = fs.mkdirs(new Path(s"$parent/$name"))
}