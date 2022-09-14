package de.unikl.cs.dbis.waves.util.operators

import org.scalatest.Inspectors._
import de.unikl.cs.dbis.waves.WavesSpec
import de.unikl.cs.dbis.waves.DataFrameFixture

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.Column

class TempColumnSpec extends WavesSpec
  with DataFrameFixture {

  "The TempColumn" should {
    "convert to the correct column" in {
      Given("A column")
      val c = new TempColumn("a")

      Then("the columns it creates refer to that name")
      c.toString should equal ("a")
      c.col should equal (col("a"))
      c.bind(df) should equal (df.col("a"))
    }

    "implicitly convert to String" in {
      def foo(s: String): Unit = ()
      "foo(TempColumn(\"a\"))" should compile
    }
    "implicitly convert to Column" in {
      def foo(s: Column): Unit = ()
      "foo(TempColumn(\"a\"))" should compile
    }

    "have unique names from the constructor" in {
      TempColumn("a") shouldNot equal (TempColumn("a"))
      TempColumn("a").toString shouldNot equal (TempColumn("a").toString())
    }
  }
}