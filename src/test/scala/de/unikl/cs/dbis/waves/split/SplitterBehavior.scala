package de.unikl.cs.dbis.waves.split

import org.scalatest.Inspectors._
import org.scalatest.PrivateMethodTester

import de.unikl.cs.dbis.waves.WavesSpec
import de.unikl.cs.dbis.waves.DataFrameFixture
import de.unikl.cs.dbis.waves.TempFolderFixture

import org.apache.spark.sql.DataFrame

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
      assert(result eq splitter)
      splitter shouldBe 'prepared
      result.getPath should equal (tempDirectory.toString)
    }
  }

  def deterministicSplitter(splitter: GroupedSplitter) = {
    "produce repeatable results" in {
      val split = PrivateMethod[Seq[DataFrame]]('split)

      splitter.prepare(df, tempDirectory.toString())
      val dfs1 = splitter invokePrivate split(df)
      val dfs2 = splitter invokePrivate split(df)
      forAll (dfs1.zip(dfs2)) { case (df1, df2) =>
        df1.collect() should contain theSameElementsAs (df1.collect)
        df2.collect() should contain theSameElementsAs (df1.collect)
      }
    }
  }
}