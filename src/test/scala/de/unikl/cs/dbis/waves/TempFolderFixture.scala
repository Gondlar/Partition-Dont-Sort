package de.unikl.cs.dbis.waves

import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.Suite

import java.nio.file.Files
import java.io.File
import java.util.UUID

import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession

trait TempFolderFixture extends BeforeAndAfterEach { this: Suite =>

  var tempDirectory : String = null

  def getFS(spark: SparkSession) = new Path(tempDirectory).getFileSystem(spark.sparkContext.hadoopConfiguration)

  override protected def beforeEach() = {
    super.beforeEach()
    val dir = Files.createTempDirectory(s"waves-test-tmp-${UUID.randomUUID()}")
    FileUtils.deleteQuietly(dir.toFile) // We need just a name, not the dir itself
    tempDirectory = dir.toString
  }
  
  override protected def afterEach(): Unit = {
    super.afterEach()
    FileUtils.deleteQuietly(new File(tempDirectory))
  }
}
