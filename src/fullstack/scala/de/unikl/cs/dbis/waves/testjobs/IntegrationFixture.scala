package de.unikl.cs.dbis.waves.testjobs

import org.scalatest.Suite
import org.scalatest.BeforeAndAfterEach
import de.unikl.cs.dbis.waves.WavesSpec
import de.unikl.cs.dbis.waves.TempFolderFixture
import org.scalatest.Inspectors._

import de.unikl.cs.dbis.waves.WavesTable._
import de.unikl.cs.dbis.waves.partitions.Bucket
import de.unikl.cs.dbis.waves.util.Logger
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession
import java.io.{File, BufferedReader, FileReader}

import scala.collection.JavaConverters._

trait IntegrationFixture extends WavesSpec
with BeforeAndAfterEach with TempFolderFixture { this: Suite =>

  var args: Array[String] = _
  var wavesPath: String = _
  var inputPath: String = _

  override def beforeEach() = {
    super.beforeEach()

    wavesPath = s"${tempDirectory.toString()}/waves"
    inputPath = "/data/twitter"
    args = Array(
      "master=local",
      s"inputPath=file://$inputPath",
      s"wavesPath=file://$wavesPath",
      s"fallbackBlocksize=${8*1024*1024}" 
    )
  }

  override def afterEach() = {
    super.afterEach()

    FileUtils.deleteQuietly(new File(Logger.logDir))
    Logger.clear()
  }

  def assertCleanedPartitions(buckets: Seq[Bucket[String]]) = {
    forAll (buckets) { bucket =>
      val dir = new File(bucket.folder(wavesPath).filename)
      dir should be ('exists)
      val files = dir.listFiles().filter(_.getName().endsWith(".parquet"))
      files should have length (1)
    }
  }

  def assertReadableResults(spark: SparkSession) = {
    val input = spark.read.json(inputPath)
    val data = spark.read.waves(wavesPath)

    data.count() should equal (input.count())
  }

  def assertLogProperties() = {
    val logs = new File("log/")
      .listFiles()
      .filter(_.toString().endsWith(".csv"))
      .toSeq
    logs should have length (1)
    val (timestamps, events, data) = new BufferedReader(new FileReader(logs.head))
      .lines().iterator().asScala
      .map(line => {
        val parts = line.split(",")
        parts should have length (3)
        noException shouldBe thrownBy (parts(0).toLong)
        (parts(0).toLong, parts(1), parts(2))
      }).toSeq.unzip3
    timestamps shouldBe sorted
    (events, data)
  }
}
