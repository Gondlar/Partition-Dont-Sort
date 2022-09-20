package de.unikl.cs.dbis.waves

import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.Suite

import java.nio.file.{Files,Path}
import java.util.UUID
import org.apache.commons.io.FileUtils

trait TempFolderFixture extends BeforeAndAfterEach { this: Suite =>

  var tempDirectory : Path = null

  override protected def beforeEach() = {
    super.beforeEach()
    tempDirectory = Files.createTempDirectory(s"waves-test-tmp-${UUID.randomUUID()}")
    FileUtils.deleteQuietly(tempDirectory.toFile()) // We need just a name, not the dir itself
  }
  
  override protected def afterEach(): Unit = {
    super.afterEach()
    FileUtils.deleteQuietly(tempDirectory.toFile())
  }
}
