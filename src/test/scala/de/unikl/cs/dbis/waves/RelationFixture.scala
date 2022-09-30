package de.unikl.cs.dbis.waves

import org.scalatest.Suite

import java.nio.file.Files
import org.apache.commons.io.FileUtils
import java.io.File
import org.apache.spark.sql.SaveMode

import WavesTable._

trait RelationFixture extends DataFrameFixture { this: Suite =>

  var directory: String = null

  override protected def beforeEach(): Unit = {
    super.beforeEach()

    directory = Files.createTempDirectory("waves_test").toString
    df.write.mode(SaveMode.Overwrite).waves(directory, df.schema)
  }

  override protected def afterEach(): Unit = {
    super.afterEach()

    FileUtils.deleteQuietly(new File(directory))
  }
}
