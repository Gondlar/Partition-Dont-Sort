package de.unikl.cs.dbis.waves.split

import de.unikl.cs.dbis.waves.WavesSpec
import de.unikl.cs.dbis.waves.RelationFixture
import de.unikl.cs.dbis.waves.TempFolderFixture
import de.unikl.cs.dbis.waves.ParquetFixture
import de.unikl.cs.dbis.waves.PartitionTreeMatchers

import java.io.File
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.hadoop.fs.Path

import de.unikl.cs.dbis.waves.WavesTable
import de.unikl.cs.dbis.waves.util.PathKey
import de.unikl.cs.dbis.waves.partitions.{PartitionTree,SplitByPresence,Bucket}
import de.unikl.cs.dbis.waves.partitions.Absent
import de.unikl.cs.dbis.waves.partitions.visitors.operations._
import de.unikl.cs.dbis.waves.partitions.PartitionMetadata
import de.unikl.cs.dbis.waves.split.recursive.{AbstractHeuristic, PartitionMetricCalculator, ColumnMetric}

import WavesTable._
import de.unikl.cs.dbis.waves.sort.NoSorter
import de.unikl.cs.dbis.waves.sort.LexicographicSorter

class RecursiveSplitterSpec extends WavesSpec
    with RelationFixture with TempFolderFixture with ParquetFixture
    with SplitterBehavior with PartitionTreeMatchers {

    "The RecursiveSplitter" should {
        behave like unpreparedSplitter(RecursiveSplitter(0, Int.MaxValue, MockHeuristic()))
        "throw an exception if getTable is called on an unprepated splitter" in {
          val splitter = RecursiveSplitter(0, Int.MaxValue, MockHeuristic())
          an [IllegalStateException] shouldBe thrownBy (splitter.getTable)
        }
        "prepare the table the given df reads from" in {
            Given("a recursive splitter and a df that reads from a WavesTable")
            val splitter = RecursiveSplitter(0, Int.MaxValue, MockHeuristic())
            val df = spark.read.waves(directory)
            val dfTable = df.getWavesTable.get

            When("we prepare that DataFrame")
            splitter.prepare(df, directory)

            Then("the prepared table is the given one")
            assert(splitter.getTable eq dfTable)
        }
        "prepare a new table if the given df reads from waves in a different dir" in {
            Given("a recursive splitter and a df that reads from a WavesTable")
            val splitter = RecursiveSplitter(0, Int.MaxValue, MockHeuristic())
            val df = spark.read.waves(directory)
            val dfTable = df.getWavesTable.get

            When("we prepare that DataFrame")
            splitter.prepare(df, tempDirectory)

            Then("the prepared table is a new one")
            val table = splitter.getTable
            table should not equal dfTable
            table.basePath should equal (tempDirectory)
        }
        "prepare a new table if the given df does not read from waves" in {
            Given("a recursive splitter and a df that reads from a WavesTable")
            val splitter = RecursiveSplitter(0, Int.MaxValue, MockHeuristic())

            When("we prepare that DataFrame")
            splitter.prepare(df, tempDirectory)

            Then("the a new table is prepared")
            val table = splitter.getTable
            table.basePath should equal (tempDirectory)
        }
        "split a table according to the heuristic" in {
            Given("A table and a recursive splitter")
            val table = WavesTable("RecursiveSplitterTest", spark, directory, CaseInsensitiveStringMap.empty)
            val splitter = RecursiveSplitter(0, Int.MaxValue, MockHeuristic()).prepare(table)
            
            When("we partition the table")
            splitter.partition()
            
            Then("The partition tree looks as expected")
            val expectedShape = SplitByPresence( "b"
                                               , SplitByPresence( "a"
                                                                , SplitByPresence("b.c", "foo", "foo")
                                                                , SplitByPresence("b.c", "foo", "foo")
                                                                )
                                               , SplitByPresence("a", "foo", "foo")
                                               )
            val expectedTree = new PartitionTree(schema, NoSorter, expectedShape)
            table.partitionTree should haveTheSameStructureAs (expectedTree)

            And("if we read the data, all is still there")
            spark.read.waves(directory).collect() should contain theSameElementsAs (data)
        }
        "use schema modifications" in {
            Given("A table and a recursive splitter")
            val table = WavesTable("RecursiveSplitterTest", spark, directory, CaseInsensitiveStringMap.empty)
            val splitter = RecursiveSplitter(0, Int.MaxValue, MockHeuristic()).prepare(table).modifySchema(true)
            
            When("we partition the table")
            splitter.partition()
            
            Then("The partition tree looks as expected")
            val expectedShape = SplitByPresence( "b"
                                               , SplitByPresence( "a"
                                                                , SplitByPresence("b.c", "foo", "foo")
                                                                , SplitByPresence("b.c", "foo", "foo")
                                                                )
                                               , SplitByPresence("a", "foo", "foo")
                                               )
            val expectedTree = new PartitionTree(schema, NoSorter, expectedShape)
            table.partitionTree should haveTheSameStructureAs (expectedTree)

            And("the schema is modified")
            val folder = table.partitionTree.root.find(Seq(Absent, Absent)).get.asInstanceOf[Bucket[String]].folder(directory)
            implicit val fs = folder.filesystem(spark)
            val file = folder.parquetFiles.next
            val parquetSchema = readParquetSchema(spark, file)
            parquetSchema.getPaths() should contain theSameElementsAs (Seq(Seq("e")))
            parquetSchema.getType(Seq("e"):_*).getRepetition().name() should equal ("REQUIRED")

            And("if we read the data, all is still there")
            spark.read.waves(directory).collect() should contain theSameElementsAs (data)
        }
        "accept the NoSorter" in {
          val splitter = new RecursiveSplitter(0, 0, null)
          val after = splitter.sortWith(NoSorter)
          after shouldBe theSameInstanceAs (splitter)
        }
        "accept no other Sorter" in {
          val splitter = new RecursiveSplitter(0, 0, null)
          an [IllegalArgumentException] shouldBe thrownBy (splitter.sortWith(LexicographicSorter))
        }
    }
}

case class MockHeuristic() extends AbstractHeuristic {

  override protected def heuristic(col: ColumnMetric): Int = ???
  
  override def choose(
    metric: PartitionMetricCalculator,
    df: DataFrame,
    metadata: PartitionMetadata,
    thresh : Double
    ): Option[PathKey]
      = Seq(PathKey("b"), PathKey("a"), PathKey("b.c"))
            .filter(!metadata.isKnown(_))
            .headOption
}
