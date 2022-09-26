package de.unikl.cs.dbis.waves.split

import de.unikl.cs.dbis.waves.WavesSpec
import de.unikl.cs.dbis.waves.DataFrameFixture
import de.unikl.cs.dbis.waves.TempFolderFixture

import org.apache.spark.sql.DataFrame

trait SplitterBehavior { this: WavesSpec with DataFrameFixture with TempFolderFixture =>

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
}
