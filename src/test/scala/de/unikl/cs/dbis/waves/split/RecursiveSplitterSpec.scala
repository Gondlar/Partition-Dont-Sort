package de.unikl.cs.dbis.waves.split

import de.unikl.cs.dbis.waves.WavesSpec
import de.unikl.cs.dbis.waves.RelationFixture
import de.unikl.cs.dbis.waves.TempFolderFixture

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import de.unikl.cs.dbis.waves.WavesTable
import de.unikl.cs.dbis.waves.WavesTable.implicits
import de.unikl.cs.dbis.waves.util.PathKey
import de.unikl.cs.dbis.waves.partitions.SplitByPresence
import de.unikl.cs.dbis.waves.partitions.Bucket
import de.unikl.cs.dbis.waves.split.recursive.{AbstractHeuristic, PartitionMetricCalculator, ColumnMetric}

class RecursiveSplitterSpec extends WavesSpec
    with RelationFixture with TempFolderFixture
    with SplitterBehavior {

    "The RecursiveSplitter" should {
        behave like unpreparedSplitter(RecursiveSplitter(0, Int.MaxValue, MockHeuristic()))
        "throw an exception if getTable is called on an unprepated splitter" in {
          val splitter = RecursiveSplitter(0, Int.MaxValue, MockHeuristic())
          an [IllegalStateException] shouldBe thrownBy (splitter.getTable)
        }
        "prepare the table the given df reads from" in {
            Given("a recursive splitter and a df that reads from a WavesTable")
            val splitter = RecursiveSplitter(0, Int.MaxValue, MockHeuristic())
            val df = spark.read.format("de.unikl.cs.dbis.waves").load(directory)
            val dfTable = df.getWavesTable.get

            When("we prepare that DataFrame")
            splitter.prepare(df, directory)

            Then("the prepared table is the given one")
            assert(splitter.getTable eq dfTable)
        }
        "prepare a new table if the given df reads from waves in a different dir" in {
            Given("a recursive splitter and a df that reads from a WavesTable")
            val splitter = RecursiveSplitter(0, Int.MaxValue, MockHeuristic())
            val df = spark.read.format("de.unikl.cs.dbis.waves").load(directory)
            val dfTable = df.getWavesTable.get

            When("we prepare that DataFrame")
            splitter.prepare(df, tempDirectory.toString)

            Then("the prepared table is a new one")
            val table = splitter.getTable
            table should not equal dfTable
            table.basePath should equal (tempDirectory.toString)
        }
        "prepare a new table if the given df does not read from waves" in {
            Given("a recursive splitter and a df that reads from a WavesTable")
            val splitter = RecursiveSplitter(0, Int.MaxValue, MockHeuristic())

            When("we prepare that DataFrame")
            splitter.prepare(df, tempDirectory.toString)

            Then("the a new table is prepared")
            val table = splitter.getTable
            table.basePath should equal (tempDirectory.toString)
        }
        // We cannot test the exact sampling amount because spark does not
        // guarantee exact numbers 
        "return a sample of the data" in {
            Given("A table and a recursive splitter")
            val table = WavesTable("RecursiveSplitterTest", spark, directory, CaseInsensitiveStringMap.empty)
            val splitter = RecursiveSplitter(0, Int.MaxValue, MockHeuristic()).prepare(table)
            
            When("we partition the table")
            splitter.partition()
            
            Then("The partition tree looks as expected")
            // Test the root
            table.partitionTree.root shouldBe a [SplitByPresence[_]]
            val root = table.partitionTree.root.asInstanceOf[SplitByPresence[_]]
            root.key should equal (PathKey("b"))

            // Test the absent side
            root.absentKey shouldBe a [SplitByPresence[_]]
            val absentSide = root.absentKey.asInstanceOf[SplitByPresence[_]]
            absentSide.key should equal (PathKey("a"))
            absentSide.absentKey shouldBe a [Bucket[_]]
            absentSide.presentKey shouldBe a [Bucket[_]]

            // Test the present side
            root.presentKey shouldBe a [SplitByPresence[_]]
            val presentSide = root.presentKey.asInstanceOf[SplitByPresence[_]]
            presentSide.key should equal (PathKey("a"))

            // We must go deeper
            presentSide.absentKey shouldBe a [SplitByPresence[_]]
            val nestedAbsentSide = presentSide.absentKey.asInstanceOf[SplitByPresence[_]]
            nestedAbsentSide.key should equal (PathKey("b.c"))
            nestedAbsentSide.absentKey shouldBe a [Bucket[_]]
            nestedAbsentSide.presentKey shouldBe a [Bucket[_]]

            presentSide.presentKey shouldBe a [SplitByPresence[_]]
            val nestedPresentSide = presentSide.presentKey.asInstanceOf[SplitByPresence[_]]
            nestedPresentSide.key should equal (PathKey("b.c"))
            nestedPresentSide.absentKey shouldBe a [Bucket[_]]
            nestedPresentSide.presentKey shouldBe a [Bucket[_]]

            And("if we read the data, all is still there")
            spark.read.format("de.unikl.cs.dbis.waves").load(directory).collect() should contain theSameElementsAs (data)
        }
    }
}

case class MockHeuristic() extends AbstractHeuristic {

  override protected def heuristic(col: ColumnMetric): Int = ???
  
  override def choose(
    metric: PartitionMetricCalculator,
    df: DataFrame,
    knownAbsent: Seq[PathKey],
    knownPresent: Seq[PathKey],
    thresh : Double
    ): Option[PathKey]
      = Seq(PathKey("b"), PathKey("a"), PathKey("b.c"))
            .filter(filterKnownPaths(knownAbsent, knownPresent, _))
            .headOption
}
