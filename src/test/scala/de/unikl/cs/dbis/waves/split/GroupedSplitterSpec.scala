package de.unikl.cs.dbis.waves.split

import org.scalatest.Inspectors._
import de.unikl.cs.dbis.waves.WavesSpec
import de.unikl.cs.dbis.waves.DataFrameFixture

import java.io.File
import org.apache.commons.io.FileUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SparkSession, DataFrame, Row}
import scala.collection.mutable.{ArrayBuilder, WrappedArray}
import de.unikl.cs.dbis.waves.util.PartitionFolder
import de.unikl.cs.dbis.waves.util.operators.{Grouper,DefinitionLevelGrouper,PresenceGrouper,NullGrouper}

class GroupedSplitterSpec extends WavesSpec
    with DataFrameFixture {

    "The GroupedSplitter" should {

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
            val sortedSets = ArrayBuilder.make[DataFrame]
            var builtSet: Seq[DataFrame] = Seq.empty
            new GroupedSplitter(testdir) {

                override protected def load(context: Unit): DataFrame = df

                override protected def splitGrouper: Grouper = DefinitionLevelGrouper

                override protected def split(df: DataFrame): Seq[DataFrame] = {
                    sortGrouper should equal (splitGrouper)
                    df.collect() should contain theSameElementsAs Seq(
                        Row(WrappedArray.make(Array(1, 1, 2)), 1),
                        Row(WrappedArray.make(Array(1, 1, 1)), 1),
                        Row(WrappedArray.make(Array(1, 0, 0)), 2),
                        Row(WrappedArray.make(Array(0, 1, 2)), 1),
                        Row(WrappedArray.make(Array(0, 1, 1)), 1),
                        Row(WrappedArray.make(Array(0, 0, 0)), 2)
                    )
                    frames
                }

                override protected def sort(bucket: DataFrame): DataFrame = {
                    sortedSets += bucket
                    super.sort(bucket)
                }

                override protected def buildTree(buckets: Seq[PartitionFolder], spark: SparkSession): Unit = ()

                override protected def write(buckets: Seq[DataFrame], rawData: DataFrame): Seq[PartitionFolder]
                    = { builtSet = buckets; Seq.empty }
            }.partition()
            sortedSets.result should contain theSameElementsInOrderAs (frames)
            builtSet should have length (frames.length)
            forAll((0 until frames.length)) { i =>
                builtSet(i).collect() should contain theSameElementsAs (frames(i).collect())
            }
        }
        "use the correct groupers" in {
          val splitter = new GroupedSplitter(testdir) {

            override protected def load(context: Unit): DataFrame = df

            override protected def splitGrouper: Grouper = PresenceGrouper
            override protected def sortGrouper: Grouper = DefinitionLevelGrouper

            override protected def split(df: DataFrame): Seq[DataFrame] = {
              df.columns should contain theSameElementsAs (PresenceGrouper.columns)
              Seq(df)
            }

            override protected def sort(df: DataFrame): DataFrame = {
              df.columns should contain theSameElementsAs (DefinitionLevelGrouper.columns)
              df
            }

            override protected def buildTree(buckets: Seq[PartitionFolder], spark: SparkSession): Unit = ()

            override protected def write(buckets: Seq[DataFrame], rawData: DataFrame): Seq[PartitionFolder] = ({
              forAll (buckets) ( df =>
                df.columns should contain theSameElementsAs (DefinitionLevelGrouper.columns)
              )
              Seq.empty
            })
          }
          splitter.partition()
        }
        "directly write single partitions correctly" in {
          Given("A GroupedSplitter")
          val splitter = TestWriteSplitter()
          splitter.doWrite = true

          When("we write one bucket")
          val folder = splitter.writeOne(df, df)

          Then("we should be able to read it again")
          spark.read.parquet(folder.filename).collect() should contain theSameElementsInOrderAs (df.collect())

          And("only the expected calls were made")
          splitter.manyCalled should equal (false)
          splitter.oneCalled should equal (true)
        }
        "directly write multiple partitions correctly" in {
          Given("A GroupedSplitter")
          val splitter = TestWriteSplitter()
          splitter.doWrite = true

          When("we write multiple buckets")
          val b1 = df.limit(3)
          val b2 = df.except(b1)
          val buckets = Seq(b1, b2)
          val folder = splitter.writeMany(buckets, df)

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
          val splitter = TestWriteSplitter()

          When("we write one bucket")
          splitter.write(Seq(df), df)

          Then("the correct method was called")
          splitter.oneCalled should equal (true)
          splitter.manyCalled should equal (false)
        }
        "write many partitions when we pass it multiple" in {
          Given("A GroupedSplitter")
          val splitter = TestWriteSplitter()

          When("we write one bucket")
          splitter.write(Seq(df,df,df), df)

          Then("the correct method was called")
          splitter.oneCalled should equal (false)
          splitter.manyCalled should equal (true)
        }
    }

    case class TestWriteSplitter() extends GroupedSplitter(testdir) {
        var doWrite = false
        var oneCalled = false
        var manyCalled = false
        override def writeOne(bucket: DataFrame, data: DataFrame): PartitionFolder = {
          oneCalled = true
          if (doWrite) super.writeOne(bucket, data) else null
        }

        override def writeMany(buckets: Seq[DataFrame], rawData: DataFrame): Seq[PartitionFolder] = {
          manyCalled = true
          if (doWrite) super.writeMany(buckets, rawData) else Seq.empty
        }
        override def write(buckets: Seq[DataFrame], rawData: DataFrame): Seq[PartitionFolder]
          = super.write(buckets, rawData)
        override protected def load(context: Unit): DataFrame = df
        override protected def splitGrouper: Grouper = NullGrouper
        override protected def split(df: DataFrame): Seq[DataFrame] = ???
        override protected def buildTree(buckets: Seq[PartitionFolder], spark: SparkSession): Unit = ???
    }

    val testdir = "test"
    override protected def afterEach(): Unit = {
      super.afterEach()

      FileUtils.deleteQuietly(new File(testdir))
    }
}
