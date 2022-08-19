package de.unikl.cs.dbis.waves.split

import de.unikl.cs.dbis.waves.WavesSpec
import de.unikl.cs.dbis.waves.Relation

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import de.unikl.cs.dbis.waves.WavesTable
import de.unikl.cs.dbis.waves.util.PathKey
import de.unikl.cs.dbis.waves.partitions.SplitByPresence
import de.unikl.cs.dbis.waves.partitions.Bucket
import de.unikl.cs.dbis.waves.split.recursive.filterKnownPaths

class RecursiveSplitterSpec extends WavesSpec
    with Relation {

    "The RecursiveSplitter" should {

        // We cannot test the exact sampling amount because spark does not
        // guarantee exact numbers 
        "return a sample of the data" in {
            Given("A table and a recursive splitter")
            val table = WavesTable("RecursiveSplitterTest", spark, directory, CaseInsensitiveStringMap.empty)
            val splitter = RecursiveSplitter(table, 0, Int.MaxValue, mockMetric)
            
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

    def mockMetric(data : DataFrame, knownAbsent : Seq[PathKey], knownPresent : Seq[PathKey], cutoff: Double)
        = Seq(PathKey("b"), PathKey("a"), PathKey("b.c"))
            .filter(filterKnownPaths(knownAbsent, knownPresent, _))
            .headOption
}
