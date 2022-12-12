package de.unikl.cs.dbis.waves.split

import org.scalatest.Inspectors._
import de.unikl.cs.dbis.waves.WavesSpec
import de.unikl.cs.dbis.waves.{DataFrameFixture, PartitionTreeFixture, TempFolderFixture}

import java.io.File
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.format.converter.ParquetMetadataConverter
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SparkSession, DataFrame, Row}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType

import de.unikl.cs.dbis.waves.partitions.PartitionTree
import de.unikl.cs.dbis.waves.partitions.Absent
import de.unikl.cs.dbis.waves.partitions.PartitionMetadata
import de.unikl.cs.dbis.waves.partitions.PartitionTreeHDFSInterface
import de.unikl.cs.dbis.waves.util.PartitionFolder
import de.unikl.cs.dbis.waves.util.PathKey
import de.unikl.cs.dbis.waves.util.operators.{Grouper,DefinitionLevelGrouper,PresenceGrouper,NullGrouper}
import de.unikl.cs.dbis.waves.sort.NoSorter
import de.unikl.cs.dbis.waves.sort.Sorter

import scala.collection.mutable.{ArrayBuilder, WrappedArray}
import scala.collection.JavaConverters._

class GroupedSplitterSpec extends WavesSpec
    with DataFrameFixture with PartitionTreeFixture with TempFolderFixture
    with SplitterBehavior {

    "The GroupedSplitter" should {
        behave like unpreparedSplitter(TestWriteSplitter(false))
        "load the prepared data" in {
          val splitter = TestWriteSplitter(false)
          splitter.prepare(df, "foo")
          splitter.load(()) should equal (df)
        }
        "process all partitions and keep their internal order" in {
            val sets = Seq( Set(data(1), data(2))
                          , Set(data(1))
                          , Set(data(2))
                          , Set(data(2), data(3))
                          )
            val frames = for (partition <- sets) yield {
                val rdd : RDD[Row] = spark.sparkContext.parallelize(partition.toSeq)
                spark.sqlContext.createDataFrame(rdd, schema)
            }
            val testMetadata = Seq.fill(frames.size)(PartitionMetadata(Seq.empty, Seq(PathKey("a")), Seq(Absent)))
            val sortedSets = ArrayBuilder.make[IntermediateData]
            var builtSet: Seq[DataFrame] = Seq.empty
            new GroupedSplitter(new Sorter {

              override val name = "test"
              override def sort(bucket: IntermediateData): IntermediateData = {
                  sortedSets += bucket
                  bucket
              }

              override def grouper: Grouper = NullGrouper

            }) {
                override protected def splitGrouper: Grouper = NullGrouper

                override protected def split(df: DataFrame): (Seq[DataFrame], Seq[PartitionMetadata]) = {
                    df.collect() should contain theSameElementsAs df.collect()
                    (frames, testMetadata)
                }

                override protected def buildTree(buckets: Seq[PartitionFolder]): PartitionTree[String] = null

                override protected def write(buckets: Seq[DataFrame]): Seq[PartitionFolder]
                    = { builtSet = buckets; Seq.empty }

                override protected def writeMetadata(tree: PartitionTree[String]): Unit = ()
            }.prepare(df, tempDirectory.toString).partition()
            val sorted = sortedSets.result
            sorted.map(_.groups) should contain theSameElementsInOrderAs (frames)
            sorted.map(_.source) should contain theSameElementsInOrderAs (frames)
            builtSet should have length (frames.length)
            forAll((0 until frames.length)) { i =>
                builtSet(i).collect() should contain theSameElementsAs (frames(i).collect())
            }
        }
        "use the correct groupers" in {
          val splitter = new GroupedSplitter(new Sorter {

            override val name = "test"

            override def sort(df: IntermediateData): IntermediateData = {
              df.groups.columns should contain theSameElementsAs (DefinitionLevelGrouper.columns)
              df
            }

            override def grouper: Grouper = DefinitionLevelGrouper
          }) with NoKnownMetadata {

            override protected def splitGrouper: Grouper = PresenceGrouper

            override protected def splitWithoutMetadata(df: DataFrame): Seq[DataFrame] = {
              df.columns should contain theSameElementsAs (PresenceGrouper.columns)
              Seq(df)
            }

            override protected def buildTree(buckets: Seq[PartitionFolder]): PartitionTree[String] = null

            override protected def write(buckets: Seq[DataFrame]): Seq[PartitionFolder] = {
              forAll (buckets) ( bucket =>
                bucket.columns should contain theSameElementsAs (df.columns)
              )
              Seq.empty
            }

            override protected def writeMetadata(tree: PartitionTree[String]): Unit = ()
          }
          splitter.prepare(df, tempDirectory.toString).partition()
        }
        "use the sorter set using sortBy" in {
          val sorter = new Sorter{
            var called = false

            override val name = "test"
            override def sort(bucket: IntermediateData): IntermediateData = {called = true; bucket}
            override def grouper: Grouper = NullGrouper
          }
          val splitter = TestWriteSplitter(false).sortWith(sorter).prepare(df,tempDirectory.toString).partition()
          sorter shouldBe 'called
        }
        "directly write single partitions correctly" in {
          Given("A GroupedSplitter")
          val splitter = TestWriteSplitter(true).prepare(df, tempDirectory.toString).asInstanceOf[TestWriteSplitter]

          When("we write one bucket")
          val folder = splitter.writeOne(df)

          Then("we should be able to read it again")
          spark.read.parquet(folder.filename).collect() should contain theSameElementsInOrderAs (df.collect())

          And("only the expected calls were made")
          splitter.manyCalled should equal (false)
          splitter.oneCalled should equal (true)
        }
        "perform schema modifications when writing" in {
          Given("A GroupedSplitter, a bucket, and its metadata")
          val metadata = PartitionMetadata(Seq.empty, Seq(PathKey("b")), Seq.empty)
          val bucket = df.filter(col("b").isNull)
          val splitter = TestWriteSplitter(true,metadata)
            .prepare(bucket, tempDirectory.toString)
            .modifySchema(true)
            .asInstanceOf[TestWriteSplitter]
          
          When("we write the bucket")
          splitter.partition()

          Then("the written file has the correct schema")
          val expectedSchema = StructType(schema.fields.filter(_.name != "b"))
          val folder = new File(tempDirectory.toString())
            .listFiles()
            .filter(file => file.isDirectory() && file.getName() != PartitionFolder.TEMP_DIR)
            .head
          val files = folder.listFiles().filter(_.getName().endsWith(".parquet"))
          files should have length (1)
          val parquetSchema = new ParquetFileReader( spark.sparkContext.hadoopConfiguration
                                                    , new Path(files.head.toString())
                                                    , ParquetMetadataConverter.NO_FILTER)
                                .getFileMetaData().getSchema()
          parquetSchema.getPaths() should contain theSameElementsAs (Seq(Seq("a"), Seq("e")))
          parquetSchema.getType(Seq("a"):_*).getRepetition().name() should equal ("OPTIONAL")
          parquetSchema.getType(Seq("e"):_*).getRepetition().name() should equal ("REQUIRED")

          And("we can read it again")
          val written = spark.createDataFrame(spark.read.schema(expectedSchema).parquet(folder.getPath()).rdd, expectedSchema)
          written.collect should contain theSameElementsInOrderAs (bucket.drop("b").collect())
          written.schema should equal (expectedSchema)
        }
        "directly write multiple partitions correctly" in {
          Given("A GroupedSplitter")
          val splitter = TestWriteSplitter(true).prepare(df, tempDirectory.toString).asInstanceOf[TestWriteSplitter]

          When("we write multiple buckets")
          val b1 = df.limit(3)
          val b2 = df.except(b1)
          val buckets = Seq(b1, b2)
          val folder = splitter.writeMany(buckets)

          Then("we should be able to read them again")
          forAll (buckets.zip(folder)) { case (bucket, folder) =>
            spark.read.parquet(folder.filename).collect() should contain theSameElementsInOrderAs (bucket.collect())
          }

          And("all expected calls were made")
          splitter.manyCalled should equal (true)
          splitter.oneCalled should equal (true)
        }
        "write one partition when we pass it one" in {
          Given("A GroupedSplitter")
          val splitter = TestWriteSplitter(false).prepare(df, tempDirectory.toString).asInstanceOf[TestWriteSplitter]

          When("we write one bucket")
          splitter.write(Seq(df))

          Then("the correct method was called")
          splitter.oneCalled should equal (true)
          splitter.manyCalled should equal (false)
        }
        "write many partitions when we pass it multiple" in {
          Given("A GroupedSplitter")
          val splitter = TestWriteSplitter(false).prepare(df, tempDirectory.toString).asInstanceOf[TestWriteSplitter]

          When("we write multiple buckets")
          splitter.write(Seq(df,df,df))

          Then("the correct method was called")
          splitter.oneCalled should equal (false)
          splitter.manyCalled should equal (true)
        }
        "write metadata to disk" in {
          Given("A partition tree and a splitter")
          val splitter = TestWriteSplitter(false).prepare(df, tempDirectory.toString).asInstanceOf[TestWriteSplitter]
          splitter.doWrite = true

          When("we write the partition tree")
          splitter.writeMetadata(spillTree)

          Then("we can read the tree back from disk")
          val read = PartitionTreeHDFSInterface(spark, tempDirectory.toString()).read()
          read should contain (spillTree)
        }
        "handle empty groups gracefully" in {
          Given("A partition tree and a splitter")
          val splitter = TestWriteSplitter(false).prepare(df, tempDirectory.toString).asInstanceOf[TestWriteSplitter]
          splitter.doWrite = true

          When("we write the partition tree")
          val result = splitter.writeMany(Seq(emptyDf, df))

          Then("we can read the contents from disk")
          val data = result.map(f => spark.read.schema(schema).parquet(f.filename).collect())
          data(0) shouldBe empty
          data(1) should contain theSameElementsAs (df.collect())
        }
    }

    case class TestWriteSplitter(var doWrite: Boolean, metadata: PartitionMetadata = PartitionMetadata()) extends GroupedSplitter {
        var oneCalled = false
        var manyCalled = false

        override def load(context: Unit): DataFrame = super.load(context)

        override def writeOne(bucket: DataFrame): PartitionFolder = {
          oneCalled = true
          if (doWrite) super.writeOne(bucket) else null
        }

        override def writeMany(buckets: Seq[DataFrame]): Seq[PartitionFolder] = {
          manyCalled = true
          if (doWrite) super.writeMany(buckets) else Seq.empty
        }
        override def write(buckets: Seq[DataFrame]): Seq[PartitionFolder]
          = super.write(buckets)

        override protected def splitGrouper: Grouper = NullGrouper
        override protected def split(df: DataFrame): (Seq[DataFrame], Seq[PartitionMetadata])
          = (Seq(df), Seq(metadata))
        override protected def buildTree(buckets: Seq[PartitionFolder]): PartitionTree[String] = new PartitionTree(schema, NoSorter)

        override def writeMetadata(tree: PartitionTree[String]): Unit
          = if(doWrite) super.writeMetadata(tree)
    }
}
