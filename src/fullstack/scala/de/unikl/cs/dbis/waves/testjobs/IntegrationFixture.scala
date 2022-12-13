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
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.format.converter.ParquetMetadataConverter
import org.apache.hadoop.fs.Path
import java.io.{File, BufferedReader, FileReader}

import scala.collection.JavaConverters._

trait IntegrationFixture extends WavesSpec
with BeforeAndAfterEach with TempFolderFixture { this: Suite =>

  var args: Array[String] = _
  var wavesPath: String = _
  var inputPath: String = _

  override protected def beforeEach() = {
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

  override protected def afterEach() = {
    super.afterEach()

    clearLogs()
  }

  def clearLogs(): Unit = {
    Logger.clear()
    FileUtils.deleteQuietly(new File(Logger.logDir))
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
    data.except(input).count() should equal (0)
    input.except(data).count() should equal (0)
  }

  def assertUnmodifiedSchema(spark: SparkSession, buckets: Seq[Bucket[String]]) = {
    val schemas = readSchemas(spark, buckets)
    if (schemas.size > 1) {
      schemas.toSet.size should equal (1)
    }
  }

  def assertModifiedSchema(spark: SparkSession, buckets: Seq[Bucket[String]]) = {
    val schemas = readSchemas(spark, buckets)
    if (buckets.size > 1) {
      schemas.toSet.size should be > 1
    }
  }

  private def readSchemas(spark: SparkSession, buckets: Seq[Bucket[String]])
    = for {bucket <- buckets;
           file <- new File(bucket.folder(wavesPath).filename).listFiles().filter(_.getName().endsWith(".parquet"))
    } yield {
      new ParquetFileReader( spark.sparkContext.hadoopConfiguration
                           , new Path(file.toString)
                           , ParquetMetadataConverter.NO_FILTER)
        .getFileMetaData().getSchema()
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
        val parts = parseLogLine(line)
        parts should have length (3)
        noException shouldBe thrownBy (parts(0).toLong)
        (parts(0).toLong, parts(1), parts(2))
      }).toSeq.unzip3
    timestamps shouldBe sorted
    (events, data)
  }

  private def parseLogLine(line: String) = {
    val tokens = line.split(",")
    var logEntries = Seq(tokens.last)
    for (token <- tokens.init.reverse) {
      val current = logEntries.head
      if (current.endsWith("'") && !current.startsWith("'")) {
        logEntries = (s"$token,$current") +: logEntries.tail
      } else {
        logEntries = token +: logEntries
      }
    }
    logEntries
  }
}
