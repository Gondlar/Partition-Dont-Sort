package de.unikl.cs.dbis.waves.util

import de.unikl.cs.dbis.waves.WavesSpec

class PartitonFolderSpec extends WavesSpec {

    "A PartitionFolder" when {
        "temporary" should {
            "be in the temp directory" in {
                val folder = new PartitionFolder("test", "foo", true)
                folder.filename should equal ("test/tmp/foo")
                folder.file.toString should equal ("test/tmp/foo")
            }
            "equal itself" in {
                val folder = new PartitionFolder("test", "foo", true)
                (folder == folder) should be (true)
                (folder == new PartitionFolder("test", "foo", true)) should be (true)
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
                (folder == folder) should be (true)
                (folder == new PartitionFolder("test", "foo", false)) should be (true)
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
            }
        }
    }   
}