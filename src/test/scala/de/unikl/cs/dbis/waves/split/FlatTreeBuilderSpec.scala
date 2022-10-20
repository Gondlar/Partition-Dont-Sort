package de.unikl.cs.dbis.waves.split

import org.scalatest.Inspectors._
import de.unikl.cs.dbis.waves.WavesSpec
import de.unikl.cs.dbis.waves.DataFrameFixture

import org.apache.spark.sql.{SparkSession, DataFrame, Row}
import de.unikl.cs.dbis.waves.partitions.{PartitionTree,Spill,Bucket}
import de.unikl.cs.dbis.waves.partitions.visitors.operations._
import de.unikl.cs.dbis.waves.util.PartitionFolder
import de.unikl.cs.dbis.waves.util.operators.{Grouper,DefinitionLevelGrouper,PresenceGrouper,NullGrouper}

class FlatTreeBuilderSpec extends WavesSpec
  with DataFrameFixture {

  "The FlatTreeBuilder" should {
    "result in a flat tree for multiple folders" in {
      Given("a sequence of partitions")
      val folders = Seq.fill(3)(PartitionFolder.makeFolder("foo", false))

      When("build a tree from them")
      val builder = TestBuilder()
      val tree = builder.buildTree(folders)

      Then("the tree should contain these folders")
      val result = tree.buckets.map(b => b.folder("foo")).toSeq
      result should contain theSameElementsAs (folders)

      And("the tree should contain spill nodes and buckets")
      tree.root.isInstanceOf[Spill[String]] shouldBe (true)
      val root = tree.root.asInstanceOf[Spill[String]]
      root.partitioned.isInstanceOf[Spill[String]] shouldBe (true)
      val child = root.partitioned.asInstanceOf[Spill[String]]
      child.partitioned.isInstanceOf[Bucket[String]] shouldBe (true)
    }
    "result in a single bucket for one folder" in {
      Given("a single of partitions")
      val folders = Seq(PartitionFolder.makeFolder("foo", false))

      When("build a tree from them")
      val builder = TestBuilder()
      val tree = builder.buildTree(folders)

      Then("the tree should contain these folders")
      val result = tree.buckets.map(b => b.folder("foo")).toSeq
      result should contain theSameElementsAs (folders)

      And("the tree should contain spill nodes and buckets")
      tree.root.isInstanceOf[Bucket[String]] shouldBe (true)
    }
  }

  case class TestBuilder() extends GroupedSplitter with FlatTreeBuilder with NoKnownMetadata {
    override protected def load(context: Unit): DataFrame = df
    override protected def splitGrouper: Grouper = ???
    override protected def splitWithoutMetadata(df: DataFrame): Seq[DataFrame] = ???

    override def buildTree(folders: Seq[PartitionFolder]): PartitionTree[String] 
      = super.buildTree(folders)
  }
}
