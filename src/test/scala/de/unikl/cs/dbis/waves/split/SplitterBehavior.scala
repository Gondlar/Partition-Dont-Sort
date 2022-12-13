package de.unikl.cs.dbis.waves.split

import org.scalatest.Inspectors._
import org.scalatest.PrivateMethodTester

import de.unikl.cs.dbis.waves.WavesSpec
import de.unikl.cs.dbis.waves.DataFrameFixture
import de.unikl.cs.dbis.waves.TempFolderFixture

import org.apache.spark.sql.DataFrame
import de.unikl.cs.dbis.waves.partitions.PartitionMetadata

trait SplitterBehavior extends PrivateMethodTester { this: WavesSpec with DataFrameFixture with TempFolderFixture =>

  def unpreparedSplitter[T](splitter: Splitter[T]) = {
    "throw an exeption if it is not prepared" when {
      "getPath is called" in {
        an [IllegalStateException] shouldBe thrownBy (splitter.getPath)
      }
      "partition is called" in {
        an [IllegalStateException] shouldBe thrownBy (splitter.partition)
      }
    }
    "be able to prepare a path" in {
      Given("a splitter")
      splitter should not be 'prepared

      When("we prepare it")
      val result = splitter.prepare(emptyDf, tempDirectory.toString)

      Then("it is prepated")
      result should be theSameInstanceAs splitter
      splitter shouldBe 'prepared
      result.getPath should equal (tempDirectory.toString)
    }
    "have finalization enabled" in {
      splitter.finalizeEnabled shouldBe (true)
    }
    "be able to toggle finalization" in {
      val step1 = splitter.doFinalize(false)
      step1 shouldBe theSameInstanceAs (splitter)
      step1.finalizeEnabled shouldBe (false)

      val step2 = step1.doFinalize(true)
      step2 shouldBe theSameInstanceAs (step1)
      step2.finalizeEnabled shouldBe (true)
    }
    "be able to toggle schema modifications" in {
      val step1 = splitter.modifySchema(true)
      step1 shouldBe theSameInstanceAs (splitter)
      step1.schemaModificationsEnabled shouldBe (true)
      
      val step2 = step1.modifySchema(false)
      step2 shouldBe theSameInstanceAs (step1)
      step2.schemaModificationsEnabled shouldBe (false)
    }
  }

  def deterministicSplitter(splitter: GroupedSplitter) = {
    "produce repeatable results" in {
      val split = PrivateMethod[(Seq[DataFrame],Seq[PartitionMetadata])]('split)

      splitter.prepare(df, tempDirectory.toString())
      val (dfs1,metadata1) = splitter invokePrivate split(df)
      val (dfs2,metadata2) = splitter invokePrivate split(df)

      metadata1 should contain theSameElementsInOrderAs (metadata2)
      forAll (dfs1.zip(dfs2)) { case (df1, df2) =>
        df1.collect() should contain theSameElementsAs (df1.collect)
        df2.collect() should contain theSameElementsAs (df1.collect)
      }
    }
  }
}