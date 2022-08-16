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
            table.partitionTree.root shouldBe a [SplitByPresence]
            val root = table.partitionTree.root.asInstanceOf[SplitByPresence]
            root.key should equal (PathKey("b"))

            // Test the absent side
            root.absentKey shouldBe a [SplitByPresence]
            val absentSide = root.absentKey.asInstanceOf[SplitByPresence]
            absentSide.key should equal (PathKey("a"))
            absentSide.absentKey shouldBe a [Bucket]
            absentSide.presentKey shouldBe a [Bucket]

            // Test the present side
            root.presentKey shouldBe a [SplitByPresence]
            val presentSide = root.presentKey.asInstanceOf[SplitByPresence]
            presentSide.key should equal (PathKey("a"))

            // We must go deeper
            presentSide.absentKey shouldBe a [SplitByPresence]
            val nestedAbsentSide = presentSide.absentKey.asInstanceOf[SplitByPresence]
            nestedAbsentSide.key should equal (PathKey("b.c"))
            nestedAbsentSide.absentKey shouldBe a [Bucket]
            nestedAbsentSide.presentKey shouldBe a [Bucket]

            presentSide.presentKey shouldBe a [SplitByPresence]
            val nestedPresentSide = presentSide.presentKey.asInstanceOf[SplitByPresence]
            nestedPresentSide.key should equal (PathKey("b.c"))
            nestedPresentSide.absentKey shouldBe a [Bucket]
            nestedPresentSide.presentKey shouldBe a [Bucket]

            And("if we read the data, all is still there")
            spark.read.format("de.unikl.cs.dbis.waves").load(directory).collect() should contain theSameElementsAs (data)
        }
    }

    def mockMetric(data : DataFrame, knownAbsent : Seq[PathKey], knownPresent : Seq[PathKey], cutoff: Double)
        = Seq(PathKey("b"), PathKey("a"), PathKey("b.c"))
            .filter(filterKnownPaths(knownAbsent, knownPresent, _))
            .headOption
}
